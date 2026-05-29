import { create } from "zustand";
import type { CdcSinkConfiguration, DiscoverResponse, TestMappingResponse } from "@/api/generated/server-api";
import {
    getTableKey,
    isTableUsable,
    type SetupWizardMessage,
    type SetupWizardStepId,
} from "@/pages/setup/add-app-wizard/wizard-model";

type SetupWizardState = {
    mappedConfiguration: CdcSinkConfiguration | null;
    schema: DiscoverResponse | null;
    selectedTableKeys: string[];
    stepMessages: Partial<Record<SetupWizardStepId, SetupWizardMessage>>;
    testResult: TestMappingResponse | null;
    clearStepMessage: (stepId: SetupWizardStepId) => void;
    reset: () => void;
    selectTableKeys: (tableKeys: string[]) => void;
    setMappedConfiguration: (configuration: CdcSinkConfiguration | null) => void;
    setSchema: (schema: DiscoverResponse | null) => void;
    setStepMessage: (stepId: SetupWizardStepId, message: SetupWizardMessage) => void;
    setTestResult: (result: TestMappingResponse | null) => void;
};

const initialState = {
    mappedConfiguration: null,
    schema: null,
    selectedTableKeys: [],
    stepMessages: {},
    testResult: null,
} satisfies Pick<
    SetupWizardState,
    "mappedConfiguration" | "schema" | "selectedTableKeys" | "stepMessages" | "testResult"
>;

export const useSetupWizardStore = create<SetupWizardState>((set) => ({
    ...initialState,
    clearStepMessage: (stepId) =>
        set((state) => {
            const nextMessages = {
                ...state.stepMessages,
            };
            delete nextMessages[stepId];

            return {
                stepMessages: nextMessages,
            };
        }),
    reset: () => set(initialState),
    selectTableKeys: (tableKeys) =>
        set({
            mappedConfiguration: null,
            selectedTableKeys: [...new Set(tableKeys)],
            testResult: null,
        }),
    setMappedConfiguration: (configuration) =>
        set({
            mappedConfiguration: configuration,
            testResult: null,
        }),
    setSchema: (schema) =>
        set({
            mappedConfiguration: null,
            schema,
            selectedTableKeys: schema
                ? schema.tables.filter((table) => isTableUsable(table)).map((table) => getTableKey(table))
                : [],
            testResult: null,
        }),
    setStepMessage: (stepId, message) =>
        set((state) => ({
            stepMessages: {
                ...state.stepMessages,
                [stepId]: message,
            },
        })),
    setTestResult: (result) =>
        set({
            testResult: result,
        }),
}));
