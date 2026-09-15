import { type MistralVibeSettings, type ProviderApprovalDecision } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

type MistralVibeAcpRuntimeSettings = Pick<MistralVibeSettings, "binaryPath">;

/**
 * Vibe owns its credentials (`vibe-acp --setup`, the `vibe` CLI, or
 * `MISTRAL_API_KEY`), so T3 advertises only what it can actually render:
 * form-based elicitation for `ask_user_question`. No `authenticate` method id
 * is sent — Vibe's only accepted methods start interactive browser sign-ins.
 */
export const MISTRAL_VIBE_CLIENT_CAPABILITIES = {
  elicitation: {
    form: {},
  },
} satisfies NonNullable<EffectAcpSchema.InitializeRequest["clientCapabilities"]>;

export function buildMistralVibeSpawnInput(
  mistralVibeSettings: MistralVibeAcpRuntimeSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: mistralVibeSettings?.binaryPath || "vibe-acp",
    args: [],
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

export interface MistralVibeAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "clientCapabilities" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly mistralVibeSettings: MistralVibeAcpRuntimeSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
}

export const makeMistralVibeAcpRuntime = (
  input: MistralVibeAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildMistralVibeSpawnInput(input.mistralVibeSettings, input.cwd, input.environment),
        clientCapabilities: MISTRAL_VIBE_CLIENT_CAPABILITIES,
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
      Effect.provide(acpContext),
    );
  });

/**
 * Selects a Vibe permission option id for a T3 decision. Vibe always offers
 * `allow_once`, a session-scoped `allow_always`, a permanent `allow_always`,
 * and `reject_once`, so selection is by kind with the session-scoped variant
 * preferred for `acceptForSession`.
 */
export function selectMistralVibePermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const option =
    decision === "acceptForSession"
      ? (request.options.find((entry) => entry.kind === "allow_always") ??
        request.options.find((entry) => entry.kind === "allow_once"))
      : decision === "accept"
        ? (request.options.find((entry) => entry.kind === "allow_once") ??
          request.options.find((entry) => entry.kind === "allow_always"))
        : request.options.find((entry) => entry.kind === "reject_once");
  const optionId = option?.optionId.trim();
  return optionId ? optionId : undefined;
}

export function selectAutoApprovedMistralVibePermissionOption(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  return (
    selectMistralVibePermissionOptionId(request, "acceptForSession") ??
    selectMistralVibePermissionOptionId(request, "accept")
  );
}

/** Vibe reports a missing API key as JSON-RPC application error -32000. */
const MISTRAL_VIBE_UNAUTHENTICATED_ERROR_CODE = -32000;

export function isMistralVibeUnauthenticatedError(cause: unknown): boolean {
  return (
    typeof cause === "object" &&
    cause !== null &&
    "_tag" in cause &&
    (cause as { readonly _tag?: unknown })._tag === "AcpRequestError" &&
    "code" in cause &&
    (cause as { readonly code?: unknown }).code === MISTRAL_VIBE_UNAUTHENTICATED_ERROR_CODE
  );
}

/** Vibe selects models through its `model` session config option. */
export function mistralVibeModelOptions(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>,
) {
  const model = configOptions.find((option) => option.id === "model");
  if (model?.type !== "select") return [];
  return model.options.flatMap((entry) => ("value" in entry ? [entry] : entry.options));
}

export function currentMistralVibeModelId(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>,
): string | undefined {
  const model = configOptions.find((option) => option.id === "model");
  return model?.type === "select" && model.currentValue ? model.currentValue : undefined;
}

/**
 * Resolves the model a turn should run on. The `mistral-vibe-default` product
 * slug is never sent to ACP — it keeps the session's current model. An explicit
 * slug is validated against the offered values so an unavailable selection
 * fails with an actionable error instead of a protocol rejection.
 */
export function resolveMistralVibeModelId(input: {
  readonly configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>;
  readonly model: string | null | undefined;
}): string | undefined {
  const current = currentMistralVibeModelId(input.configOptions);
  const requested = input.model?.trim();
  if (!requested || requested === "mistral-vibe-default") {
    return current;
  }
  const available = mistralVibeModelOptions(input.configOptions).some(
    (option) => option.value === requested,
  );
  return available ? requested : current;
}

export const applyMistralVibeAcpModelSelection = Effect.fn("applyMistralVibeAcpModelSelection")(
  function* <E>(input: {
    readonly runtime: Pick<AcpSessionRuntime.AcpSessionRuntime["Service"], "setModel">;
    readonly model: string | null | undefined;
    readonly mapError: (cause: EffectAcpErrors.AcpError) => E;
  }): Effect.fn.Return<string | undefined, E> {
    const requested = input.model?.trim();
    if (!requested || requested === "mistral-vibe-default") {
      return undefined;
    }
    yield* input.runtime.setModel(requested).pipe(Effect.mapError(input.mapError));
    return requested;
  },
);

/**
 * Vibe projects its `ask_user_question` tool onto ACP form elicitation: one
 * property per question, options encoded as `oneOf` (single) or `anyOf` on an
 * array (multi-select). The elicitation schema has no "type your own answer"
 * option, so every question is marked custom-answer-capable and an answer
 * outside the offered labels flows through as the "other" answer.
 */
export function extractMistralVibeUserInputQuestions(
  request: EffectAcpSchema.ElicitationRequest,
): ReadonlyArray<{
  readonly id: string;
  readonly header: string;
  readonly question: string;
  readonly options: ReadonlyArray<{ readonly label: string; readonly description: string }>;
  readonly allowCustomAnswer: boolean;
  readonly multiSelect: boolean;
}> {
  if (request.mode !== "form") {
    return [];
  }
  const fallbackQuestion = request.message.trim() || "Answer the question.";
  return Object.entries(request.requestedSchema.properties ?? {}).map(([key, property]) => {
    const labelOptions: ReadonlyArray<EffectAcpSchema.EnumOption> =
      property.type === "string"
        ? (property.oneOf ?? (property.enum ?? []).map((value) => ({ const: value, title: value })))
        : property.type === "array"
          ? "anyOf" in property.items
            ? property.items.anyOf
            : []
          : [];
    return {
      id: key,
      header: (property.title ?? key).trim() || key,
      question: (property.description ?? fallbackQuestion).trim() || fallbackQuestion,
      options: labelOptions.map((option) => ({
        label: (option.title ?? option.const).trim(),
        description: "",
      })),
      allowCustomAnswer: true,
      multiSelect: property.type === "array",
    };
  });
}

/** Coerces a UI answer into the primitive values ACP elicitation accepts. */
function coerceElicitationValue(value: unknown): EffectAcpSchema.ElicitationContentValue {
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => (typeof entry === "string" ? entry : String(entry)));
  }
  return String(value);
}

/** Builds the elicitation reply for resolved user-input answers. */
export function buildMistralVibeElicitationResponse(
  request: EffectAcpSchema.ElicitationRequest,
  answers: Readonly<Record<string, unknown>>,
): EffectAcpSchema.ElicitationResponse {
  if (request.mode !== "form") {
    return { action: { action: "decline" } };
  }
  const content: Record<string, EffectAcpSchema.ElicitationContentValue> = {};
  for (const key of Object.keys(request.requestedSchema.properties ?? {})) {
    const answer = answers[key];
    if (answer === undefined) {
      continue;
    }
    content[key] = coerceElicitationValue(answer);
  }
  return Object.keys(content).length > 0
    ? { action: { action: "accept", content } }
    : { action: { action: "decline" } };
}
