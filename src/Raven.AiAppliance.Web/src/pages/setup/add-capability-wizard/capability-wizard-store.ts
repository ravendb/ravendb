import { create } from "zustand";
import type { AiAgentConfiguration } from "@/api/generated/server-api";

export type CapabilityWizardState = {
    // AI-suggested agent candidates returned by the suggest/agent endpoint. Held here (not in
    // the form) because they are read-only source data the create/review steps select from.
    suggestions: AiAgentConfiguration[];
    setSuggestions: (suggestions: AiAgentConfiguration[]) => void;
    reset: () => void;
};

const initialState: Pick<CapabilityWizardState, "suggestions"> = {
    suggestions: [],
};

export const useCapabilityWizardStore = create<CapabilityWizardState>((set) => ({
    ...initialState,
    setSuggestions: (suggestions) => set({ suggestions }),
    reset: () => set(initialState),
}));
