import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect, useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { z } from "zod";
import { ChooseDataSourceStep } from "@/pages/setup/add-app-wizard/steps/choose-data-source-step";
import { ConnectSourceStep, useConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect-source-step";
import { MapSchemaStep, useMapSchemaStep } from "@/pages/setup/add-app-wizard/steps/map-schema-step";
import { PreviewStep, usePreviewStep } from "@/pages/setup/add-app-wizard/steps/preview-step";
import { useVerifySchemaStep, VerifySchemaStep } from "@/pages/setup/add-app-wizard/steps/verify-schema-step";
import { WizardLayout } from "@/pages/setup/add-app-wizard/wizard-layout";
import {
    getInitialFormValues,
    getPreviousStep,
    getStepIndex,
    type SetupWizardFormValues,
    type SetupWizardStepId,
} from "@/pages/setup/add-app-wizard/wizard-model";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";

const setupWizardSchema = z.object({
    appName: z.string().trim().min(1, "Application name is required."),
    connectionString: z.string().trim().min(1, "Connection string is required."),
    dataSource: z.literal("external"),
    mappingMode: z.literal("auto"),
    provider: z.string().trim().min(1, "Database type is required."),
});

export function AddAppWizard() {
    const [currentStep, setCurrentStep] = useState<SetupWizardStepId>("choose-source");
    const stepMessages = useSetupWizardStore((state) => state.stepMessages);
    const resetWizard = useSetupWizardStore((state) => state.reset);
    const form = useForm<SetupWizardFormValues>({
        mode: "onChange",
        defaultValues: getInitialFormValues(),
        resolver: zodResolver(setupWizardSchema),
    });

    useEffect(() => {
        resetWizard();

        return resetWizard;
    }, [resetWizard]);

    return (
        <FormProvider {...form}>
            <AddAppWizardContent currentStep={currentStep} onStepChange={setCurrentStep} stepMessages={stepMessages} />
        </FormProvider>
    );
}

function AddAppWizardContent({
    currentStep,
    onStepChange,
    stepMessages,
}: {
    currentStep: SetupWizardStepId;
    onStepChange: (step: SetupWizardStepId) => void;
    stepMessages: ReturnType<typeof useSetupWizardStore.getState>["stepMessages"];
}) {
    const connectSourceStep = useConnectSourceStep();
    const verifySchemaStep = useVerifySchemaStep();
    const mapSchemaStep = useMapSchemaStep();
    const previewStep = usePreviewStep();
    const isWorking =
        connectSourceStep.isWorking || verifySchemaStep.isWorking || mapSchemaStep.isWorking || previewStep.isWorking;

    async function handleNext() {
        switch (currentStep) {
            case "choose-source":
                onStepChange("connect-source");
                return;
            case "connect-source":
                if (await connectSourceStep.connectAndDiscoverSource()) {
                    onStepChange("verify-schema");
                }
                return;
            case "verify-schema":
                if (await verifySchemaStep.completeStep()) {
                    onStepChange("map-schema");
                }
                return;
            case "map-schema":
                if (await mapSchemaStep.completeStep()) {
                    onStepChange("preview");
                }
                return;
            case "preview":
                await previewStep.completeStep();
                return;
        }
    }

    return (
        <WizardLayout
            canGoBack={getStepIndex(currentStep) > 0}
            currentStep={currentStep}
            isWorking={isWorking}
            nextLabel={getNextLabel(currentStep)}
            onBack={() => onStepChange(getPreviousStep(currentStep))}
            onNext={() => void handleNext()}
        >
            {currentStep === "choose-source" && <ChooseDataSourceStep message={stepMessages["choose-source"]} />}
            {currentStep === "connect-source" && (
                <ConnectSourceStep isWorking={isWorking} message={stepMessages["connect-source"]} />
            )}
            {currentStep === "verify-schema" && (
                <VerifySchemaStep
                    isWorking={isWorking}
                    message={stepMessages["verify-schema"]}
                    onDiscoverSchema={() => void verifySchemaStep.discoverSchema()}
                    onVerifyConnection={() => void verifySchemaStep.verifyConnection()}
                />
            )}
            {currentStep === "map-schema" && (
                <MapSchemaStep
                    isWorking={isWorking}
                    message={stepMessages["map-schema"]}
                    onPrepareMapping={() => void mapSchemaStep.prepareAutoMapping()}
                />
            )}
            {currentStep === "preview" && (
                <PreviewStep
                    isWorking={isWorking}
                    message={stepMessages.preview}
                    onRunPreview={() => void previewStep.runPreview()}
                />
            )}
        </WizardLayout>
    );
}

function getNextLabel(currentStep: SetupWizardStepId) {
    switch (currentStep) {
        case "choose-source":
        case "verify-schema":
        case "map-schema":
            return "Next";
        case "connect-source":
            return "Verify and next";
        case "preview":
            return "Setup your application";
    }
}
