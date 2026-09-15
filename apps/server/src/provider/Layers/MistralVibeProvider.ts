import {
  MISTRAL_VIBE_DEFAULT_MODEL,
  type MistralVibeSettings,
  type ModelCapabilities,
  type ServerProviderAuth,
  type ServerProviderModel,
} from "@t3tools/contracts";
import type * as EffectAcpSchema from "effect-acp/schema";
import * as EffectAcpErrors from "effect-acp/errors";
import { causeErrorTag } from "@t3tools/shared/observability";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  isMistralVibeUnauthenticatedError,
  makeMistralVibeAcpRuntime,
} from "../acp/MistralVibeAcpSupport.ts";
import {
  buildServerProvider,
  COMPACT_SLASH_COMMAND,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const MISTRAL_VIBE_PRESENTATION = {
  displayName: "Mistral Vibe",
  supportsConversationRollback: false,
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;
// Spawning `vibe-acp` boots Python and loads the app-server; generous timeout.
const MISTRAL_VIBE_SESSION_PROBE_TIMEOUT_MS = 30_000;

const MISTRAL_VIBE_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: MISTRAL_VIBE_DEFAULT_MODEL,
    name: "Default",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

export function buildInitialMistralVibeProviderSnapshot(
  mistralVibeSettings: MistralVibeSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = mistralVibeModelsFromSettings(mistralVibeSettings.customModels);

    if (!mistralVibeSettings.enabled) {
      return buildServerProvider({
        presentation: MISTRAL_VIBE_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Mistral Vibe is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: MISTRAL_VIBE_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Mistral Vibe availability...",
      },
    });
  });
}

function mistralVibeModelsFromSettings(
  customModels: MistralVibeSettings["customModels"],
  discovered: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    discovered.length > 0 ? discovered : MISTRAL_VIBE_BUILT_IN_MODELS,
    customModels,
    EMPTY_CAPABILITIES,
  );
}

const runMistralVibeCliCommand = (
  mistralVibeSettings: MistralVibeSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.gen(function* () {
    const command = mistralVibeSettings.binaryPath || "vibe-acp";
    const spawnCommand = yield* resolveSpawnCommand(command, args, { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

/**
 * Vibe offers models through its `model` session config option; the option's
 * current value is what the session would run on, so it becomes the default.
 */
export function buildMistralVibeModelsFromConfigOptions(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption> | null | undefined,
): ReadonlyArray<ServerProviderModel> {
  const model = configOptions?.find((option) => option.id === "model");
  if (model?.type !== "select") {
    return [];
  }
  const seen = new Set<string>();
  return model.options.flatMap((entry) => {
    const values = "value" in entry ? [entry] : entry.options;
    return values.flatMap((option): ServerProviderModel[] => {
      const slug = option.value.trim();
      if (!slug || seen.has(slug)) {
        return [];
      }
      seen.add(slug);
      return [
        {
          slug,
          name: option.name.trim() || slug,
          isCustom: false,
          ...(option.value === model.currentValue ? { isDefault: true } : {}),
          capabilities: EMPTY_CAPABILITIES,
        },
      ];
    });
  });
}

interface MistralVibeSessionProbeResult {
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly version: string | null;
}

/**
 * Starts one probe session to read auth (a `-32000` ACP error means the API
 * key is missing) and the model config option. The session is deleted on the
 * way out so probes do not accumulate in Vibe's global session history.
 */
const probeMistralVibeSession = (
  mistralVibeSettings: MistralVibeSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
): Effect.Effect<
  MistralVibeSessionProbeResult,
  EffectAcpErrors.AcpError,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> =>
  Effect.gen(function* () {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const acp = yield* makeMistralVibeAcpRuntime({
      mistralVibeSettings,
      environment,
      childProcessSpawner,
      cwd,
      clientInfo: { name: "t3-code-provider-probe", version: "0.0.0" },
    });
    const started = yield* acp.start();
    // Extension methods are underscore-prefixed on the wire (ACP extensibility).
    yield* Effect.ignore(acp.request("_session/delete", { sessionId: started.sessionId }));
    return {
      models: buildMistralVibeModelsFromConfigOptions(yield* acp.getConfigOptions),
      version: started.initializeResult.agentInfo?.version?.trim() || null,
    };
  }).pipe(Effect.scoped);

export const checkMistralVibeProviderStatus = Effect.fn("checkMistralVibeProviderStatus")(
  function* (
    mistralVibeSettings: MistralVibeSettings,
    environment: NodeJS.ProcessEnv = process.env,
    cwd?: string,
  ): Effect.fn.Return<
    ServerProviderDraft,
    never,
    ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
  > {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const fallbackModels = mistralVibeModelsFromSettings(mistralVibeSettings.customModels);

    if (!mistralVibeSettings.enabled) {
      return buildServerProvider({
        presentation: MISTRAL_VIBE_PRESENTATION,
        enabled: false,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Mistral Vibe is disabled in T3 Code settings.",
        },
      });
    }

    const versionResult = yield* runMistralVibeCliCommand(
      mistralVibeSettings,
      ["--version"],
      environment,
    ).pipe(Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS), Effect.result);

    if (Result.isFailure(versionResult)) {
      const error = versionResult.failure;
      yield* Effect.logWarning("Mistral Vibe health check failed.", {
        errorTag: error._tag,
      });
      return buildServerProvider({
        presentation: MISTRAL_VIBE_PRESENTATION,
        enabled: mistralVibeSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: isCommandMissingCause(error)
            ? "Mistral Vibe (`vibe-acp`) is not installed or not on PATH. Install the `mistral-vibe` package and try again."
            : "Failed to execute the Mistral Vibe health check.",
        },
      });
    }

    if (Option.isNone(versionResult.success)) {
      return buildServerProvider({
        presentation: MISTRAL_VIBE_PRESENTATION,
        enabled: mistralVibeSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "Mistral Vibe is installed but timed out while running `vibe-acp --version`.",
        },
      });
    }

    const versionOutput = versionResult.success.value;
    const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);

    const sessionProbe = yield* probeMistralVibeSession(
      mistralVibeSettings,
      environment,
      cwd ?? process.cwd(),
    ).pipe(Effect.timeoutOption(MISTRAL_VIBE_SESSION_PROBE_TIMEOUT_MS), Effect.exit);

    if (Exit.isFailure(sessionProbe)) {
      const cause = sessionProbe.cause;
      const error = Option.getOrUndefined(Cause.findErrorOption(cause));
      if (error !== undefined && isMistralVibeUnauthenticatedError(error)) {
        return buildServerProvider({
          presentation: MISTRAL_VIBE_PRESENTATION,
          enabled: mistralVibeSettings.enabled,
          checkedAt,
          models: fallbackModels,
          probe: {
            installed: true,
            version,
            status: "error",
            auth: { status: "unauthenticated" },
            message:
              "Mistral Vibe is not signed in. Run `vibe-acp --setup` (or sign in with the `vibe` CLI), or set MISTRAL_API_KEY.",
          },
        });
      }
      yield* Effect.logWarning("Mistral Vibe ACP session probe failed.", {
        errorTag: causeErrorTag(cause),
      });
      return buildServerProvider({
        presentation: MISTRAL_VIBE_PRESENTATION,
        enabled: mistralVibeSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version,
          status: "warning",
          auth: { status: "unknown" },
          message:
            "Mistral Vibe is installed but its ACP agent did not start. Model options may be incomplete.",
        },
      });
    }

    if (Option.isNone(sessionProbe.value)) {
      return buildServerProvider({
        presentation: MISTRAL_VIBE_PRESENTATION,
        enabled: mistralVibeSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version,
          status: "warning",
          auth: { status: "unknown" },
          message: "Mistral Vibe ACP session probe timed out. Model options may be incomplete.",
        },
      });
    }

    const { models: discoveredModels, version: acpVersion } = sessionProbe.value.value;
    const models = mistralVibeModelsFromSettings(
      mistralVibeSettings.customModels,
      discoveredModels,
    );
    const auth: ServerProviderAuth = { status: "authenticated" };

    return buildServerProvider({
      presentation: MISTRAL_VIBE_PRESENTATION,
      enabled: mistralVibeSettings.enabled,
      checkedAt,
      models,
      slashCommands: [COMPACT_SLASH_COMMAND],
      probe: {
        installed: true,
        version: version ?? acpVersion,
        status: discoveredModels.length === 0 ? "warning" : "ready",
        auth,
        ...(discoveredModels.length === 0
          ? { message: "Mistral Vibe did not report any models. The default model is used." }
          : {}),
      },
    });
  },
);
