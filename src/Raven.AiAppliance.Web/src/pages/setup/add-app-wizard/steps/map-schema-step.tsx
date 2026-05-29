/* eslint-disable react-refresh/only-export-components */
import { useMutation } from "@tanstack/react-query";
import { Bot, SlidersHorizontal, Upload, WandSparkles } from "lucide-react";
import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import { Button } from "@/components/shadcn/ui/button";
import { cn } from "@/lib/utils";
import { MappingTable } from "@/pages/setup/add-app-wizard/mapping-table";
import {
    buildAutoConfiguration,
    firstMessage,
    MAPPING_MODE_OPTIONS,
    toConnectRequest,
    type SetupWizardFormValues,
    type SetupWizardMessage,
} from "@/pages/setup/add-app-wizard/wizard-model";
import {
    isDiscoveredSchemaReady,
    runWizardRequest,
    setWizardMessage,
} from "@/pages/setup/add-app-wizard/wizard-request-utils";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";

export function MapSchemaStep({
    message,
    onPrepareMapping,
    isWorking,
}: {
    isWorking: boolean;
    message?: SetupWizardMessage;
    onPrepareMapping: () => void;
}) {
    const mappedConfiguration = useSetupWizardStore((state) => state.mappedConfiguration);
    const schema = useSetupWizardStore((state) => state.schema);
    const selectedTableKeys = useSetupWizardStore((state) => state.selectedTableKeys);

    return (
        <StepSection
            title="Map your schema"
            description="Choose how source tables become RavenDB documents."
            message={message}
        >
            <div className="grid gap-5">
                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                    {MAPPING_MODE_OPTIONS.map((option) => {
                        const Icon = getMappingModeIcon(option.id);
                        const isSelected = option.id === "auto";

                        return (
                            <button
                                key={option.id}
                                type="button"
                                disabled={option.disabled}
                                aria-pressed={isSelected}
                                className={cn(
                                    "min-h-32 rounded-lg border bg-background p-4 text-left transition-colors",
                                    "hover:bg-accent hover:text-accent-foreground",
                                    isSelected && "border-foreground bg-accent text-accent-foreground",
                                    option.disabled && "cursor-not-allowed opacity-55 hover:bg-background",
                                )}
                            >
                                <Icon className="mb-5 size-5" aria-hidden="true" />
                                <span className="block text-sm font-semibold">{option.label}</span>
                                <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                                    {option.description}
                                </span>
                            </button>
                        );
                    })}
                </div>

                <div className="flex justify-end">
                    <Button
                        type="button"
                        variant="secondary"
                        onClick={onPrepareMapping}
                        disabled={isWorking || !schema || selectedTableKeys.length === 0}
                    >
                        <WandSparkles className="size-4" aria-hidden="true" />
                        Generate auto mapping
                    </Button>
                </div>

                <MappingTable configuration={mappedConfiguration} />
            </div>
        </StepSection>
    );
}

export function useMapSchemaStep() {
    const form = useFormContext<SetupWizardFormValues>();
    const schema = useSetupWizardStore((state) => state.schema);
    const mappedConfiguration = useSetupWizardStore((state) => state.mappedConfiguration);
    const setMappedConfiguration = useSetupWizardStore((state) => state.setMappedConfiguration);
    const setSchema = useSetupWizardStore((state) => state.setSchema);
    const clearStepMessage = useSetupWizardStore((state) => state.clearStepMessage);
    const discoverSchemaMutation = useMutation({
        mutationFn: (values: SetupWizardFormValues) => api.services.setup.discover(toConnectRequest(values)),
    });
    const mapConfigurationMutation = useMutation({
        mutationFn: (configuration: ReturnType<typeof buildAutoConfiguration>) => api.services.setup.map(configuration),
    });

    async function prepareAutoMapping() {
        clearStepMessage("map-schema");

        if (!(await form.trigger(["provider", "connectionString"]))) {
            return false;
        }

        if (mappedConfiguration) {
            return true;
        }

        const sourceSchema = isDiscoveredSchemaReady(schema)
            ? schema
            : await runWizardRequest("map-schema", () => discoverSchemaMutation.mutateAsync(form.getValues()));

        if (!sourceSchema) {
            return false;
        }

        if (sourceSchema.errors?.length) {
            setWizardMessage("map-schema", {
                title: "Schema discovery failed.",
                description: firstMessage(sourceSchema.errors),
                type: "error",
            });
            return false;
        }

        setSchema(sourceSchema);

        const configuration = buildAutoConfiguration(sourceSchema, useSetupWizardStore.getState().selectedTableKeys);

        if (!configuration.tables?.length) {
            setWizardMessage("map-schema", {
                title: "No CDC-ready tables were discovered.",
                description: "Auto mapping needs tables with primary keys and capturable CDC columns.",
                type: "error",
            });
            return false;
        }

        const mapped = await runWizardRequest("map-schema", () => mapConfigurationMutation.mutateAsync(configuration));

        if (!mapped) {
            return false;
        }

        setMappedConfiguration(mapped);
        setWizardMessage("map-schema", {
            title: "Mapping prepared.",
            description: `${mapped.tables?.length ?? 0} tables are ready for preview.`,
            type: "success",
        });
        return true;
    }

    async function completeStep() {
        if (mappedConfiguration || (await prepareAutoMapping())) {
            setWizardMessage("preview", {
                title: "Mapping is ready.",
                description: "Review the generated configuration before starting the load.",
                type: "success",
            });
            return true;
        }

        return false;
    }

    return {
        completeStep,
        isWorking: discoverSchemaMutation.isPending || mapConfigurationMutation.isPending,
        prepareAutoMapping,
    };
}

function getMappingModeIcon(optionId: (typeof MAPPING_MODE_OPTIONS)[number]["id"]) {
    switch (optionId) {
        case "auto":
            return WandSparkles;
        case "ai-suggest":
            return Bot;
        case "manual":
            return SlidersHorizontal;
        case "import":
            return Upload;
    }
}
