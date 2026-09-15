import { describe, expect, it } from "vite-plus/test";
import type * as EffectAcpSchema from "effect-acp/schema";

import {
  buildMistralVibeElicitationResponse,
  buildMistralVibeSpawnInput,
  extractMistralVibeUserInputQuestions,
  isMistralVibeUnauthenticatedError,
  resolveMistralVibeModelId,
  selectAutoApprovedMistralVibePermissionOption,
  selectMistralVibePermissionOptionId,
} from "./MistralVibeAcpSupport.ts";

const permissionRequest = (options: ReadonlyArray<{ optionId: string; kind: string }>) =>
  ({
    options: options.map((option) => ({
      optionId: option.optionId,
      name: option.optionId,
      kind: option.kind,
    })),
  }) as unknown as EffectAcpSchema.RequestPermissionRequest;

const modelConfigOptions = (values: ReadonlyArray<string>, current: string) =>
  [
    {
      id: "model",
      name: "Model",
      category: "model",
      type: "select",
      currentValue: current,
      options: values.map((value) => ({ value, name: value })),
    },
  ] as unknown as ReadonlyArray<EffectAcpSchema.SessionConfigOption>;

describe("buildMistralVibeSpawnInput", () => {
  it("defaults to vibe-acp on PATH", () => {
    expect(buildMistralVibeSpawnInput(null, "/tmp/cwd")).toEqual({
      command: "vibe-acp",
      args: [],
      cwd: "/tmp/cwd",
    });
  });

  it("uses the configured binary path and environment", () => {
    expect(
      buildMistralVibeSpawnInput({ binaryPath: "/opt/vibe-acp" }, "/tmp/cwd", {
        MISTRAL_API_KEY: "k",
      }),
    ).toEqual({
      command: "/opt/vibe-acp",
      args: [],
      cwd: "/tmp/cwd",
      env: { MISTRAL_API_KEY: "k" },
    });
  });
});

describe("selectMistralVibePermissionOptionId", () => {
  const request = permissionRequest([
    { optionId: "allow_once", kind: "allow_once" },
    { optionId: "allow_always", kind: "allow_always" },
    { optionId: "allow_always_permanent", kind: "allow_always" },
    { optionId: "reject_once", kind: "reject_once" },
  ]);
  const onlyAlways = permissionRequest([{ optionId: "allow_always", kind: "allow_always" }]);

  it("maps each T3 decision to the matching Vibe option", () => {
    expect(selectMistralVibePermissionOptionId(request, "accept")).toBe("allow_once");
    expect(selectMistralVibePermissionOptionId(request, "acceptForSession")).toBe("allow_always");
    expect(selectMistralVibePermissionOptionId(request, "decline")).toBe("reject_once");
  });

  it("falls back across allow kinds when Vibe omits one", () => {
    const onlyOnce = permissionRequest([{ optionId: "allow_once", kind: "allow_once" }]);
    expect(selectMistralVibePermissionOptionId(onlyOnce, "acceptForSession")).toBe("allow_once");
    expect(selectMistralVibePermissionOptionId(onlyAlways, "accept")).toBe("allow_always");
  });

  it("prefers session-scoped allow for full-access auto-approval", () => {
    expect(selectAutoApprovedMistralVibePermissionOption(request)).toBe("allow_always");
    expect(selectAutoApprovedMistralVibePermissionOption(onlyAlways)).toBe("allow_always");
  });
});

describe("isMistralVibeUnauthenticatedError", () => {
  it("matches Vibe's -32000 application error", () => {
    expect(isMistralVibeUnauthenticatedError({ _tag: "AcpRequestError", code: -32000 })).toBe(true);
    expect(isMistralVibeUnauthenticatedError({ _tag: "AcpRequestError", code: -32602 })).toBe(
      false,
    );
    expect(isMistralVibeUnauthenticatedError(new Error("nope"))).toBe(false);
  });
});

describe("resolveMistralVibeModelId", () => {
  const configOptions = modelConfigOptions(["mistral-small", "mistral-large"], "mistral-small");

  it("keeps the session model for the product slug", () => {
    expect(
      resolveMistralVibeModelId({
        configOptions,
        model: "mistral-vibe-default",
      }),
    ).toBe("mistral-small");
  });

  it("passes an available selection through", () => {
    expect(resolveMistralVibeModelId({ configOptions, model: "mistral-large" })).toBe(
      "mistral-large",
    );
  });

  it("falls back to the current model for an unavailable selection", () => {
    expect(resolveMistralVibeModelId({ configOptions, model: "mistral-huge" })).toBe(
      "mistral-small",
    );
  });
});

describe("extractMistralVibeUserInputQuestions", () => {
  it("projects Vibe's form elicitation onto T3 questions", () => {
    const request = {
      mode: "form",
      message: "Pick one",
      sessionId: "s1",
      requestedSchema: {
        properties: {
          q0: {
            type: "string",
            title: "Language",
            description: "Which language?",
            oneOf: [
              { const: "ts", title: "TypeScript" },
              { const: "rs", title: "Rust" },
            ],
          },
        },
        required: ["q0"],
      },
    } as unknown as EffectAcpSchema.ElicitationRequest;

    expect(extractMistralVibeUserInputQuestions(request)).toEqual([
      {
        id: "q0",
        header: "Language",
        question: "Which language?",
        options: [
          { label: "TypeScript", description: "" },
          { label: "Rust", description: "" },
        ],
        allowCustomAnswer: true,
        multiSelect: false,
      },
    ]);
  });

  it("marks array properties as multi-select and supports freeform questions", () => {
    const request = {
      mode: "form",
      message: "Anything else?",
      sessionId: "s1",
      requestedSchema: {
        properties: {
          q0: { type: "array", items: { anyOf: [{ const: "a", title: "A" }] } },
          q1: { type: "boolean" },
        },
      },
    } as unknown as EffectAcpSchema.ElicitationRequest;

    const questions = extractMistralVibeUserInputQuestions(request);
    expect(questions[0]).toMatchObject({ id: "q0", multiSelect: true, allowCustomAnswer: true });
    expect(questions[1]).toMatchObject({
      id: "q1",
      question: "Anything else?",
      options: [],
    });
  });

  it("declines non-form (URL) elicitation", () => {
    const request = {
      mode: "url",
      elicitationId: "e1",
      url: "https://example.com",
      message: "Sign in",
      sessionId: "s1",
    } as unknown as EffectAcpSchema.ElicitationRequest;
    expect(extractMistralVibeUserInputQuestions(request)).toEqual([]);
    expect(buildMistralVibeElicitationResponse(request, {})).toEqual({
      action: { action: "decline" },
    });
  });
});

describe("buildMistralVibeElicitationResponse", () => {
  const request = {
    mode: "form",
    message: "Pick one",
    sessionId: "s1",
    requestedSchema: {
      properties: { q0: { type: "string" }, q1: { type: "array" } },
    },
  } as unknown as EffectAcpSchema.ElicitationRequest;

  it("accepts with the answered content only", () => {
    expect(
      buildMistralVibeElicitationResponse(request, { q0: "ts", q1: ["a", "b"], other: "x" }),
    ).toEqual({ action: { action: "accept", content: { q0: "ts", q1: ["a", "b"] } } });
  });

  it("declines when no requested property was answered", () => {
    expect(buildMistralVibeElicitationResponse(request, {})).toEqual({
      action: { action: "decline" },
    });
    expect(buildMistralVibeElicitationResponse(request, { unrelated: "x" })).toEqual({
      action: { action: "decline" },
    });
  });
});
