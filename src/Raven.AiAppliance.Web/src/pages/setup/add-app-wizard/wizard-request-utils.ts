import type { DiscoverResponse } from "@/api/generated/server-api";
import {
    firstMessage,
    isConnectSuccess,
    type SetupWizardMessage,
    type SetupWizardStepId,
} from "@/pages/setup/add-app-wizard/wizard-model";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";

export async function runWizardRequest<T>(messageStepId: SetupWizardStepId, action: () => Promise<T>) {
    try {
        return await action();
    } catch (error) {
        useSetupWizardStore.getState().setStepMessage(messageStepId, {
            title: "Setup request failed.",
            description: error instanceof Error ? error.message : undefined,
            type: "error",
        });
        return null;
    }
}

export function setWizardMessage(stepId: SetupWizardStepId, message: SetupWizardMessage) {
    useSetupWizardStore.getState().setStepMessage(stepId, message);
}

export function isDiscoveredSchemaReady(value: DiscoverResponse | null): value is DiscoverResponse {
    return Boolean(value && !value.errors?.length && value.tables?.length);
}

export function setConnectionResultMessage(
    messageStepId: SetupWizardStepId,
    result: { success: boolean; errors: string[]; warnings: string[] },
) {
    if (!isConnectSuccess(result)) {
        setWizardMessage(messageStepId, {
            title: "Connection failed.",
            description: firstMessage(result.errors) ?? "Connection verification failed.",
            type: "error",
        });
        return false;
    }

    setWizardMessage(messageStepId, {
        title: "Success! Your connection string works properly.",
        description: firstMessage(result.warnings),
        type: "success",
    });
    return true;
}
