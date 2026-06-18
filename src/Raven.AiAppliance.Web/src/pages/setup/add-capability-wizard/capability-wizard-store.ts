import { create } from "zustand";
import type { AiAgentConfiguration } from "@/api/generated/server-api";

// The agent configuration generated from a free-text prompt, kept with the prompt it was
// generated from. Stored separately from `suggestions` (it is not one of the data-derived
// cards) and reused as the provisioning base for "prompt" mode. The paired prompt lets the
// wizard skip regenerating when the text is unchanged.
export type PromptResult = {
    prompt: string;
    config: AiAgentConfiguration;
};

export type CreatedAgent = {
    agentId: string;
    name: string;
};

export type CapabilityWizardState = {
    // AI-suggested agent candidates returned by the suggest/agent endpoint. Held here (not in
    // the form) because they are read-only source data the create/review steps select from.
    suggestions: AiAgentConfiguration[];
    setSuggestions: (suggestions: AiAgentConfiguration[]) => void;
    // The latest agent generated from a custom prompt (see PromptResult).
    promptResult: PromptResult | null;
    setPromptResult: (result: PromptResult) => void;
    createdAgent: CreatedAgent | null;
    setCreatedAgent: (agent: CreatedAgent) => void;
    reset: () => void;
};

const initialState: Pick<CapabilityWizardState, "suggestions" | "promptResult" | "createdAgent"> = {
    suggestions: [],
    promptResult: null,
    createdAgent: null,
};

export const useCapabilityWizardStore = create<CapabilityWizardState>((set) => ({
    ...initialState,
    setSuggestions: (suggestions) => set({ suggestions }),
    setPromptResult: (promptResult) => set({ promptResult }),
    setCreatedAgent: (createdAgent) => set({ createdAgent }),
    reset: () => set(initialState),
}));
