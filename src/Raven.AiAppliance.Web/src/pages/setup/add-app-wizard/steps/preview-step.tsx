/* eslint-disable react-refresh/only-export-components */
import { useMutation } from "@tanstack/react-query";
import { Play } from "lucide-react";
import { useFormContext, useWatch } from "react-hook-form";
import { useNavigate } from "react-router";
import { api } from "@/api/api";
import type { CdcSinkConfiguration, TestMappingResponse } from "@/api/generated/server-api";
import { Button } from "@/components/shadcn/ui/button";
import { MappingTable } from "@/pages/setup/add-app-wizard/mapping-table";
import { MessageList } from "@/pages/setup/add-app-wizard/schema-table";
import {
    firstMessage,
    PROVIDER_OPTIONS,
    type SetupWizardFormValues,
    type SetupWizardMessage,
} from "@/pages/setup/add-app-wizard/wizard-model";
import { runWizardRequest, setWizardMessage } from "@/pages/setup/add-app-wizard/wizard-request-utils";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";

export function PreviewStep({
    isWorking,
    message,
    onRunPreview,
}: {
    isWorking: boolean;
    message?: SetupWizardMessage;
    onRunPreview: () => void;
}) {
    const { control } = useFormContext<SetupWizardFormValues>();
    const mappedConfiguration = useSetupWizardStore((state) => state.mappedConfiguration);
    const testResult = useSetupWizardStore((state) => state.testResult);
    const appName = useWatch({
        control,
        name: "appName",
    });
    const provider = useWatch({
        control,
        name: "provider",
    });
    const providerLabel = PROVIDER_OPTIONS.find((option) => option.value === provider)?.label ?? provider;

    return (
        <StepSection title="Preview" description="Review the source, schema, and generated mapping." message={message}>
            <div className="grid gap-5">
                <div className="grid gap-3 md:grid-cols-3">
                    <SummaryPanel label="Application" value={appName || "Untitled"} />
                    <SummaryPanel label="Source" value={providerLabel} />
                    <SummaryPanel label="Mapped tables" value={String(mappedConfiguration?.tables?.length ?? 0)} />
                </div>

                <MappingTable configuration={mappedConfiguration} />

                <div className="flex justify-end">
                    <Button
                        type="button"
                        variant="secondary"
                        onClick={onRunPreview}
                        disabled={isWorking || !mappedConfiguration}
                    >
                        <Play className="size-4" aria-hidden="true" />
                        Run preview
                    </Button>
                </div>

                <MappingPreviewResult result={testResult} />
            </div>
        </StepSection>
    );
}

export function usePreviewStep() {
    const navigate = useNavigate();
    const form = useFormContext<SetupWizardFormValues>();
    const mappedConfiguration = useSetupWizardStore((state) => state.mappedConfiguration);
    const setTestResult = useSetupWizardStore((state) => state.setTestResult);
    const clearStepMessage = useSetupWizardStore((state) => state.clearStepMessage);
    const testMappingMutation = useMutation({
        mutationFn: ({
            maxRows,
            table,
        }: {
            maxRows: number | null;
            table: NonNullable<CdcSinkConfiguration["tables"]>[number];
        }) =>
            api.services.setup.testMapping({
                maxRows,
                sourceTableName: table.sourceTableName ?? "",
                sourceTableSchema: table.sourceTableSchema,
            }),
    });
    const provisionMutation = useMutation({
        mutationFn: (appName: string) =>
            api.services.setup.provision({
                appName,
            }),
    });

    async function runPreview() {
        clearStepMessage("preview");

        if (!(await form.trigger(["provider", "connectionString"]))) {
            return false;
        }

        if (!mappedConfiguration) {
            setWizardMessage("preview", {
                title: "No mapped table available.",
                description: "Map at least one table before running preview.",
                type: "error",
            });
            return false;
        }

        return await requestPreview(mappedConfiguration);
    }

    async function completeStep() {
        clearStepMessage("preview");

        if (!(await form.trigger("appName")) || !(await form.trigger(["provider", "connectionString"]))) {
            return false;
        }

        if (!mappedConfiguration) {
            setWizardMessage("preview", {
                title: "No mapped table available.",
                description: "Map at least one table before running preview.",
                type: "error",
            });
            return false;
        }

        const currentTestResult = useSetupWizardStore.getState().testResult;

        if (!currentTestResult || currentTestResult.errors.length > 0) {
            const previewSucceeded = await requestPreview(mappedConfiguration);

            if (!previewSucceeded) {
                return false;
            }
        }

        const values = form.getValues();
        const result = await runWizardRequest("preview", () => provisionMutation.mutateAsync(values.appName.trim()));

        if (!result) {
            return false;
        }

        navigate(`/apps/${result.slug}`);
        return true;
    }

    async function requestPreview(configuration: CdcSinkConfiguration) {
        const table = configuration.tables?.[0];

        if (!table) {
            setWizardMessage("preview", {
                title: "No mapped table available.",
                description: "Map at least one table before running preview.",
                type: "error",
            });
            return false;
        }

        if (!table.sourceTableName) {
            setWizardMessage("preview", {
                title: "Mapped table is incomplete.",
                description: "Select a source table before running preview.",
                type: "error",
            });
            return false;
        }

        const result = await runWizardRequest("preview", () =>
            testMappingMutation.mutateAsync({
                maxRows: 1,
                table,
            }),
        );

        if (!result) {
            return false;
        }

        setTestResult(result);

        if (result.errors?.length) {
            setWizardMessage("preview", {
                title: "Mapping preview failed.",
                description: firstMessage(result.errors),
                type: "error",
            });
            return false;
        }

        setWizardMessage("preview", {
            title: "Preview completed.",
            description: `${result.results?.length ?? 0} rows returned.`,
            type: "success",
        });
        return true;
    }

    return {
        completeStep,
        isWorking: testMappingMutation.isPending || provisionMutation.isPending,
        runPreview,
    };
}

function MappingPreviewResult({ result }: { result: TestMappingResponse | null }) {
    if (!result) {
        return (
            <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
                Preview has not been run yet.
            </div>
        );
    }

    return (
        <div className="grid gap-3">
            <MessageList messages={[...result.errors, ...result.warnings]} tone="destructive" />
            {result.results.length === 0 ? (
                <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
                    No preview rows returned.
                </div>
            ) : (
                result.results.map((row, index) => (
                    <pre key={index} className="max-h-64 overflow-auto rounded-lg border bg-background p-3 text-xs">
                        {row.error || row.document || row.sourceRow || "Empty result"}
                    </pre>
                ))
            )}
        </div>
    );
}

function SummaryPanel({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-lg border bg-background p-4">
            <p className="text-xs font-medium text-muted-foreground">{label}</p>
            <p className="mt-2 truncate text-sm font-semibold">{value}</p>
        </div>
    );
}
