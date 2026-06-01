import { create } from "zustand";
import type { DiscoverResponse } from "@/api/generated/server-api";

export type SetupWizardState = {
    reset: () => void;
    discoverResult: DiscoverResponse | null;
    setDiscoverResult: (x: DiscoverResponse) => void;
};

const initialState: Pick<SetupWizardState, "discoverResult"> = {
    discoverResult: null,
};

export const useSetupWizardStore = create<SetupWizardState>((set) => ({
    ...initialState,
    reset: () => set(initialState),
    setDiscoverResult: (result) =>
        set({
            discoverResult: result,
        }),
}));
