/**
 * MistralVibeAdapter — shape type for the Mistral Vibe provider adapter.
 *
 * Retained as a naming anchor for the driver bundle, mirroring the Cursor
 * adapter shape.
 *
 * @module MistralVibeAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/** MistralVibeAdapterShape — per-instance Mistral Vibe adapter contract. */
export interface MistralVibeAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
