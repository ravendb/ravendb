/* eslint-disable react-refresh/only-export-components */
import { useMutation } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { FormTextarea } from "@/components/form/form-textarea";
import {
    firstMessage,
    PROVIDER_OPTIONS,
    toConnectRequest,
    toVerifyConnectRequest,
    type SetupWizardFormValues,
    type SetupWizardMessage,
} from "@/pages/setup/add-app-wizard/wizard-model";
import {
    runWizardRequest,
    setConnectionResultMessage,
    setWizardMessage,
} from "@/pages/setup/add-app-wizard/wizard-request-utils";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";

export function ConnectSourceStep({ isWorking, message }: { isWorking: boolean; message?: SetupWizardMessage }) {
    const { control } = useFormContext<SetupWizardFormValues>();

    return (
        <StepSection
            title="Connect to your source database"
            description="Enter the external database connection details."
            message={message}
        >
            <div className="grid gap-5">
                <FormInput
                    control={control}
                    name="appName"
                    label="Application name"
                    placeholder="e.g. AcmeShop"
                    disabled={isWorking}
                />
                <FormSelect
                    control={control}
                    name="provider"
                    label="Database type"
                    options={PROVIDER_OPTIONS}
                    disabled={isWorking}
                />
                <FormTextarea
                    control={control}
                    name="connectionString"
                    label="Connection string"
                    placeholder="Host=localhost;Port=5432;Database=my_db;Username=admin;Password=pass"
                    textareaClassName="font-mono text-xs"
                />
            </div>
        </StepSection>
    );
}

export function useConnectSourceStep() {
    const form = useFormContext<SetupWizardFormValues>();
    const clearStepMessage = useSetupWizardStore((state) => state.clearStepMessage);
    const setSchema = useSetupWizardStore((state) => state.setSchema);
    const discoverSchemaMutation = useMutation({
        mutationFn: (values: SetupWizardFormValues) => api.services.setup.discover(toConnectRequest(values)),
    });
    const verifyConnectionMutation = useMutation({
        mutationFn: ({
            values,
            schema,
        }: {
            values: SetupWizardFormValues;
            schema: NonNullable<ReturnType<typeof useSetupWizardStore.getState>["schema"]>;
        }) => api.services.setup.connect(toVerifyConnectRequest(values, schema)),
    });

    async function connectAndDiscoverSource() {
        clearStepMessage("connect-source");

        if (
            !(await form.trigger("appName", { shouldFocus: true })) ||
            !(await form.trigger(["provider", "connectionString"], { shouldFocus: true }))
        ) {
            return false;
        }

        const values = form.getValues();
        const discoveredSchema = await runWizardRequest("connect-source", () =>
            discoverSchemaMutation.mutateAsync(values),
        );

        if (!discoveredSchema) {
            return false;
        }

        setSchema(discoveredSchema);

        if (discoveredSchema.errors?.length) {
            setWizardMessage("connect-source", {
                title: "Schema discovery failed.",
                description: firstMessage(discoveredSchema.errors),
                type: "error",
            });
            return false;
        }

        if (discoveredSchema.tables?.length === 0) {
            setWizardMessage("connect-source", {
                title: "No tables were discovered.",
                description: "Check the source database permissions and selected table filters.",
                type: "error",
            });
            return false;
        }

        const result = await runWizardRequest("connect-source", () =>
            verifyConnectionMutation.mutateAsync({ schema: discoveredSchema, values }),
        );

        if (!result || !setConnectionResultMessage("connect-source", result)) {
            return false;
        }

        setWizardMessage("verify-schema", {
            title: "Source verified.",
            description: `${discoveredSchema.tables.length} tables were discovered and checked.`,
            type: "success",
        });
        return true;
    }

    return {
        connectAndDiscoverSource,
        isWorking: discoverSchemaMutation.isPending || verifyConnectionMutation.isPending,
    };
}
