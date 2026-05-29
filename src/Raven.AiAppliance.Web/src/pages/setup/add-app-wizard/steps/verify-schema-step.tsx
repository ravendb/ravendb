/* eslint-disable react-refresh/only-export-components */
import { useMutation } from "@tanstack/react-query";
import { Search, TestTube2 } from "lucide-react";
import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { Button } from "@/components/shadcn/ui/button";
import { SchemaTable } from "@/pages/setup/add-app-wizard/schema-table";
import {
    firstMessage,
    toConnectRequest,
    toVerifyConnectRequest,
    type SetupWizardFormValues,
    type SetupWizardMessage,
} from "@/pages/setup/add-app-wizard/wizard-model";
import {
    isDiscoveredSchemaReady,
    runWizardRequest,
    setConnectionResultMessage,
    setWizardMessage,
} from "@/pages/setup/add-app-wizard/wizard-request-utils";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";

export function VerifySchemaStep({
    isWorking,
    message,
    onDiscoverSchema,
    onVerifyConnection,
}: {
    isWorking: boolean;
    message?: SetupWizardMessage;
    onDiscoverSchema: () => void;
    onVerifyConnection: () => void;
}) {
    return (
        <StepSection
            title="Verify your schema"
            description="Tables from the default schema are discovered and verified automatically."
            message={message}
        >
            <div className="grid gap-4">
                <div className="flex flex-wrap justify-end gap-2">
                    <Button type="button" variant="secondary" onClick={onDiscoverSchema} disabled={isWorking}>
                        <Search className="size-4" aria-hidden="true" />
                        Discover tables
                    </Button>
                    <Button type="button" variant="secondary" onClick={onVerifyConnection} disabled={isWorking}>
                        <TestTube2 className="size-4" aria-hidden="true" />
                        Verify source
                    </Button>
                </div>
                <SchemaTable />
            </div>
        </StepSection>
    );
}

export function useVerifySchemaStep() {
    const form = useFormContext<SetupWizardFormValues>();
    const schema = useSetupWizardStore((state) => state.schema);
    const clearStepMessage = useSetupWizardStore((state) => state.clearStepMessage);
    const setSchema = useSetupWizardStore((state) => state.setSchema);
    const discoverSchemaMutation = useMutation({
        mutationFn: (values: SetupWizardFormValues) => api.services.setup.discover(toConnectRequest(values)),
    });
    const verifyConnectionMutation = useMutation({
        mutationFn: ({ schema, values }: { schema: DiscoverResponse; values: SetupWizardFormValues }) =>
            api.services.setup.connect(toVerifyConnectRequest(values, schema)),
    });

    async function discoverSchema() {
        clearStepMessage("verify-schema");

        if (!(await form.trigger(["provider", "connectionString"]))) {
            return false;
        }

        const discoveredSchema = await runWizardRequest("verify-schema", () =>
            discoverSchemaMutation.mutateAsync(form.getValues()),
        );

        if (!discoveredSchema) {
            return false;
        }

        setSchema(discoveredSchema);

        if (discoveredSchema.errors?.length) {
            setWizardMessage("verify-schema", {
                title: "Schema discovery failed.",
                description: firstMessage(discoveredSchema.errors),
                type: "error",
            });
            return false;
        }

        if (discoveredSchema.tables?.length === 0) {
            setWizardMessage("verify-schema", {
                title: "No tables were discovered.",
                description: "Check the source database permissions and selected table filters.",
                type: "error",
            });
            return false;
        }

        setWizardMessage("verify-schema", {
            title: "Schema discovered.",
            description: `${discoveredSchema.tables?.length ?? 0} tables found in the default schema.`,
            type: "success",
        });
        return true;
    }

    async function verifyConnection() {
        clearStepMessage("verify-schema");

        if (!(await form.trigger(["provider", "connectionString"]))) {
            return false;
        }

        const sourceSchema = schema ?? ((await discoverSchema()) ? useSetupWizardStore.getState().schema : null);

        if (!sourceSchema) {
            return false;
        }

        if (!sourceSchema.tables.length) {
            setWizardMessage("verify-schema", {
                title: "No tables to verify.",
                description: "Discover source tables before running verification.",
                type: "error",
            });
            return false;
        }

        const result = await runWizardRequest("verify-schema", () =>
            verifyConnectionMutation.mutateAsync({ schema: sourceSchema, values: form.getValues() }),
        );

        return Boolean(result && setConnectionResultMessage("verify-schema", result));
    }

    async function completeStep() {
        const hasSchema = isDiscoveredSchemaReady(schema) || (await discoverSchema());
        const selectedTableKeys = useSetupWizardStore.getState().selectedTableKeys;

        if (hasSchema && selectedTableKeys.length > 0) {
            setWizardMessage("map-schema", {
                title: "Schema is ready.",
                description: "Auto mapping can now generate a CDC configuration.",
                type: "success",
            });
            return true;
        }

        setWizardMessage("verify-schema", {
            title: "Select at least one table.",
            description: "Choose the verified source tables that should be used by this application.",
            type: "error",
        });
        return false;
    }

    return {
        completeStep,
        discoverSchema,
        isWorking: discoverSchemaMutation.isPending || verifyConnectionMutation.isPending,
        verifyConnection,
    };
}
