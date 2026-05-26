import { zodResolver } from "@hookform/resolvers/zod";
import { useState } from "react";
import { useForm } from "react-hook-form";
import { useNavigate } from "react-router";
import { z } from "zod";
import { api } from "@/api/api";
import type {
    CdcSinkConfiguration,
    CdcSinkSourceSchema,
    ProvisionResult,
    TestMappingResult,
} from "@/api/setup-service";
import { WizardLayout } from "@/pages/setup/add-app-wizard/wizard-layout";
import {
    buildAutoConfiguration,
    DESCRIPTION_MAX_LENGTH,
    firstMessage,
    getInitialFormValues,
    getPreviousStep,
    getStepIndex,
    isConnectSuccess,
    parseTableNames,
    toConnectRequest,
    type SetupWizardMessage,
    type SetupWizardFormValues,
    type SetupWizardStepId,
} from "@/pages/setup/add-app-wizard/wizard-model";
import {
    BasicConfigurationStep,
    ChooseDataSourceStep,
    ConnectSourceStep,
    LoadProgressStep,
    MapSchemaStep,
    PreviewStep,
    VerifySchemaStep,
} from "@/pages/setup/add-app-wizard/wizard-steps";

const setupWizardSchema = z.object({
    appName: z.string().trim().min(1, "Application name is required."),
    connectionString: z.string().trim().min(1, "Connection string is required."),
    dataSource: z.literal("external"),
    description: z.string().max(DESCRIPTION_MAX_LENGTH, `Use ${DESCRIPTION_MAX_LENGTH} characters or fewer.`),
    mappingMode: z.literal("auto"),
    maxRows: z.number().min(1, "Use at least one row.").nullable(),
    provider: z.string().trim().min(1, "Database type is required."),
    tableNames: z.array(
        z.object({
            name: z.string(),
        }),
    ),
});

export function AddAppWizard() {
    const navigate = useNavigate();
    const [currentStep, setCurrentStep] = useState<SetupWizardStepId>("choose-source");
    const [schema, setSchema] = useState<CdcSinkSourceSchema | null>(null);
    const [mappedConfiguration, setMappedConfiguration] = useState<CdcSinkConfiguration | null>(null);
    const [testResult, setTestResult] = useState<TestMappingResult | null>(null);
    const [provisionResult, setProvisionResult] = useState<ProvisionResult | null>(null);
    const [stepMessages, setStepMessages] = useState<Partial<Record<SetupWizardStepId, SetupWizardMessage>>>({});
    const [isWorking, setIsWorking] = useState(false);
    const form = useForm<SetupWizardFormValues>({
        defaultValues: getInitialFormValues(),
        resolver: zodResolver(setupWizardSchema),
    });

    async function handleNext() {
        switch (currentStep) {
            case "choose-source":
                setCurrentStep("basic-configuration");
                return;
            case "basic-configuration":
                if (await triggerBasicConfigurationFields("basic-configuration")) {
                    setCurrentStep("connect-source");
                }
                return;
            case "connect-source":
                if (await verifyConnection("connect-source")) {
                    setStepMessage("verify-schema", {
                        title: "Source verified.",
                        description: "You can discover tables from the linked source database.",
                        type: "success",
                    });
                    setCurrentStep("verify-schema");
                }
                return;
            case "verify-schema":
                if (isDiscoveredSchemaReady(schema) || (await discoverSchema())) {
                    setStepMessage("map-schema", {
                        title: "Schema is ready.",
                        description: "Auto mapping can now generate a CDC configuration.",
                        type: "success",
                    });
                    setCurrentStep("map-schema");
                }
                return;
            case "map-schema":
                if (mappedConfiguration || (await prepareAutoMapping())) {
                    setStepMessage("preview", {
                        title: "Mapping is ready.",
                        description: "Review the generated configuration before starting the load.",
                        type: "success",
                    });
                    setCurrentStep("preview");
                }
                return;
            case "preview":
                await startLoad();
                return;
            case "load-progress":
                if (provisionResult) {
                    navigate(`/apps/${provisionResult.slug}`);
                }
                return;
        }
    }

    function handleBack() {
        setCurrentStep(getPreviousStep(currentStep));
    }

    async function verifyConnection(messageStepId: SetupWizardStepId = "connect-source") {
        if (!(await triggerConnectionFields(messageStepId))) {
            return false;
        }

        const values = form.getValues();

        return await runBusy(async () => {
            const result = await api.services.setup.connect(toConnectRequest(values));

            if (!isConnectSuccess(result)) {
                setStepMessage(messageStepId, {
                    title: "Connection failed.",
                    description: firstMessage(result.errors) ?? "Connection verification failed.",
                    type: "error",
                });
                return false;
            }

            setStepMessage(messageStepId, {
                title: "Success! Your connection string works properly.",
                description: firstMessage(result.warnings),
                type: "success",
            });
            return true;
        });
    }

    async function discoverSchema() {
        if (!(await triggerConnectionFields("verify-schema"))) {
            return false;
        }

        const values = form.getValues();

        return await runBusy(async () => Boolean(await requestSchema(values, "verify-schema")));
    }

    async function prepareAutoMapping() {
        if (!(await triggerConnectionFields("map-schema"))) {
            return false;
        }

        const values = form.getValues();

        return await runBusy(async () => Boolean(await ensureMappedConfiguration(values, "map-schema")));
    }

    async function runPreview() {
        if (!(await triggerConnectionFields("preview"))) {
            return false;
        }

        const values = form.getValues();

        return await runBusy(async () => {
            const configuration = await ensureMappedConfiguration(values, "preview");

            if (!configuration) {
                return false;
            }

            return await requestPreview(configuration, values);
        });
    }

    async function startLoad() {
        if (!(await triggerBasicConfigurationFields("preview")) || !(await triggerConnectionFields("preview"))) {
            return false;
        }

        const values = form.getValues();

        return await runBusy(async () => {
            const configuration = await ensureMappedConfiguration(values, "preview");

            if (!configuration) {
                return false;
            }

            if (!testResult || testResult.errors.length > 0) {
                const previewSucceeded = await requestPreview(configuration, values);

                if (!previewSucceeded) {
                    return false;
                }
            }

            const result = await api.services.setup.provision({
                appName: values.appName.trim(),
            });
            setProvisionResult(result);
            setStepMessage("load-progress", {
                title: "App provisioned.",
                description: "The initial load has started.",
                type: "success",
            });
            setCurrentStep("load-progress");
            return true;
        });
    }

    async function requestSchema(values: SetupWizardFormValues, messageStepId: SetupWizardStepId) {
        const discoveredSchema = await api.services.setup.discover(toConnectRequest(values));
        setSchema(discoveredSchema);
        setMappedConfiguration(null);
        setTestResult(null);

        if (discoveredSchema.errors.length > 0) {
            setStepMessage(messageStepId, {
                title: "Schema discovery failed.",
                description: firstMessage(discoveredSchema.errors),
                type: "error",
            });
            return null;
        }

        if (discoveredSchema.tables.length === 0) {
            setStepMessage(messageStepId, {
                title: "No tables were discovered.",
                description: "Check the source database permissions and selected table filters.",
                type: "error",
            });
            return null;
        }

        setStepMessage(messageStepId, {
            title: "Schema discovered.",
            description: `${discoveredSchema.tables.length} tables found.`,
            type: "success",
        });
        return discoveredSchema;
    }

    async function ensureMappedConfiguration(values: SetupWizardFormValues, messageStepId: SetupWizardStepId) {
        if (mappedConfiguration) {
            return mappedConfiguration;
        }

        const sourceSchema = isDiscoveredSchemaReady(schema) ? schema : await requestSchema(values, messageStepId);

        if (!sourceSchema) {
            return null;
        }

        const configuration = buildAutoConfiguration(sourceSchema, parseTableNames(values.tableNames));

        if (configuration.tables.length === 0) {
            setStepMessage(messageStepId, {
                title: "No CDC-ready tables were discovered.",
                description: "Auto mapping needs tables with primary keys and capturable CDC columns.",
                type: "error",
            });
            return null;
        }

        const mapped = await api.services.setup.map(configuration);
        setMappedConfiguration(mapped);
        setTestResult(null);
        setStepMessage(messageStepId, {
            title: "Mapping prepared.",
            description: `${mapped.tables.length} tables are ready for preview.`,
            type: "success",
        });
        return mapped;
    }

    async function requestPreview(configuration: CdcSinkConfiguration, values: SetupWizardFormValues) {
        const table = configuration.tables[0];

        if (!table) {
            setStepMessage("preview", {
                title: "No mapped table available.",
                description: "Map at least one table before running preview.",
                type: "error",
            });
            return false;
        }

        const result = await api.services.setup.testMapping({
            maxRows: values.maxRows,
            sourceTableName: table.sourceTableName,
            sourceTableSchema: table.sourceTableSchema,
        });
        setTestResult(result);

        if (result.errors.length > 0) {
            setStepMessage("preview", {
                title: "Mapping preview failed.",
                description: firstMessage(result.errors),
                type: "error",
            });
            return false;
        }

        setStepMessage("preview", {
            title: "Preview completed.",
            description: `${result.results.length} rows returned.`,
            type: "success",
        });
        return true;
    }

    async function runBusy(action: () => Promise<boolean>) {
        setIsWorking(true);

        try {
            return await action();
        } catch (error) {
            setStepMessage(currentStep, {
                title: "Setup request failed.",
                description: error instanceof Error ? error.message : undefined,
                type: "error",
            });
            return false;
        } finally {
            setIsWorking(false);
        }
    }

    async function triggerBasicConfigurationFields(messageStepId: SetupWizardStepId) {
        clearStepMessage(messageStepId);

        return await form.trigger(["appName", "description"], {
            shouldFocus: currentStep === "basic-configuration",
        });
    }

    async function triggerConnectionFields(messageStepId: SetupWizardStepId) {
        clearStepMessage(messageStepId);

        return await form.trigger(["provider", "connectionString"], {
            shouldFocus: currentStep === "connect-source",
        });
    }

    function setStepMessage(stepId: SetupWizardStepId, message: SetupWizardMessage) {
        setStepMessages((current) => ({
            ...current,
            [stepId]: message,
        }));
    }

    function clearStepMessage(stepId: SetupWizardStepId) {
        setStepMessages((current) => {
            const nextMessages = {
                ...current,
            };
            delete nextMessages[stepId];
            return nextMessages;
        });
    }

    return (
        <WizardLayout
            canGoBack={getStepIndex(currentStep) > 0 && currentStep !== "load-progress"}
            currentStep={currentStep}
            isWorking={isWorking}
            nextLabel={getNextLabel(currentStep)}
            onBack={handleBack}
            onNext={() => void handleNext()}
        >
            {currentStep === "choose-source" && <ChooseDataSourceStep message={stepMessages["choose-source"]} />}
            {currentStep === "basic-configuration" && (
                <BasicConfigurationStep
                    control={form.control}
                    isWorking={isWorking}
                    message={stepMessages["basic-configuration"]}
                />
            )}
            {currentStep === "connect-source" && (
                <ConnectSourceStep
                    control={form.control}
                    isWorking={isWorking}
                    message={stepMessages["connect-source"]}
                    onVerifyConnection={() => void verifyConnection()}
                />
            )}
            {currentStep === "verify-schema" && (
                <VerifySchemaStep
                    control={form.control}
                    isWorking={isWorking}
                    message={stepMessages["verify-schema"]}
                    onDiscoverSchema={() => void discoverSchema()}
                    onVerifyConnection={() => void verifyConnection("verify-schema")}
                    schema={schema}
                />
            )}
            {currentStep === "map-schema" && (
                <MapSchemaStep
                    control={form.control}
                    isWorking={isWorking}
                    mappedConfiguration={mappedConfiguration}
                    message={stepMessages["map-schema"]}
                    onPrepareMapping={() => void prepareAutoMapping()}
                    schema={schema}
                />
            )}
            {currentStep === "preview" && (
                <PreviewStep
                    control={form.control}
                    isWorking={isWorking}
                    mappedConfiguration={mappedConfiguration}
                    message={stepMessages.preview}
                    onRunPreview={() => void runPreview()}
                    schema={schema}
                    testResult={testResult}
                />
            )}
            {currentStep === "load-progress" && (
                <LoadProgressStep
                    mappedConfiguration={mappedConfiguration}
                    message={stepMessages["load-progress"]}
                    provisionResult={provisionResult}
                />
            )}
        </WizardLayout>
    );
}

function getNextLabel(currentStep: SetupWizardStepId) {
    switch (currentStep) {
        case "choose-source":
        case "basic-configuration":
        case "verify-schema":
        case "map-schema":
            return "Next";
        case "connect-source":
            return "Verify and next";
        case "preview":
            return "Start load";
        case "load-progress":
            return "Open app";
    }
}

function isDiscoveredSchemaReady(value: CdcSinkSourceSchema | null): value is CdcSinkSourceSchema {
    return Boolean(value && value.errors.length === 0 && value.tables.length > 0);
}
