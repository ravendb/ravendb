import { SetupWizardStepId, SetupWizardSetupMethod, SetupWizardSecurityOption } from "../setupWizardValidation";
import {
    SetupWizardAdditionalSettingsStep,
    SetupWizardAdditionalSettingsStepFooter,
} from "../steps/SetupWizardAdditionalSettingsStep";
import { SetupWizardDomainStep, SetupWizardDomainStepFooter } from "../steps/SetupWizardDomainStep";
import { SetupWizardEulaStep, SetupWizardEulaStepFooter } from "../steps/SetupWizardEulaStep";
import { SetupWizardFinishStep, SetupWizardFinishStepFooter } from "../steps/SetupWizardFinishStep";
import { SetupWizardLicenseKeyStep, SetupWizardLicenseKeyStepFooter } from "../steps/SetupWizardLicenseKeyStep";
import { SetupWizardNodeAddressStep, SetupWizardNodeAddressStepFooter } from "../steps/SetupWizardNodeAddressStep";
import { SetupWizardSecurityStep, SetupWizardSecurityStepFooter } from "../steps/SetupWizardSecurityStep";
import {
    SetupWizardSelfSignedCertificateStep,
    SetupWizardSelfSignedCertificateStepFooter,
} from "../steps/SetupWizardSelfSignedCertificateStep";
import { SetupWizardSetupMethodStep, SetupWizardSetupMethodStepFooter } from "../steps/SetupWizardSetupMethodStep";
import { SetupWizardSummaryStep, SetupWizardSummaryStepFooter } from "../steps/SetupWizardSummaryStep";
import { SetupWizardUsePackageStep, SetupWizardUsePackageStepFooter } from "../steps/SetupWizardUsePackageStep";
import { useAppSelector } from "components/store";
import { setupWizardSelectors } from "components/setupWizard/store/setupWizardSlice";

export interface SetupWizardStep {
    title: SetupWizardStepId;
    description: string;
    component: React.ReactNode;
    footer: React.ReactNode;
    isCurrent?: boolean;
    isAvailable?: boolean;
    isVisible?: boolean;
}

export interface SetupWizardStepsProps {
    currentStep: SetupWizardStepId;
    setupMethod: SetupWizardSetupMethod;
    securityOption: SetupWizardSecurityOption;
}

export function useSetupWizardSteps({
    currentStep,
    setupMethod,
    securityOption,
}: SetupWizardStepsProps): SetupWizardStep[] {
    const finishStatus = useAppSelector(setupWizardSelectors.finishStepStatus);
    const getIsNotInStepIds = (stepIds: SetupWizardStepId[]) => !stepIds.some((x) => currentStep === x);

    const steps: SetupWizardStep[] = [
        {
            title: "Eula",
            description: "RavenDB Studio Eula",
            component: <SetupWizardEulaStep />,
            footer: <SetupWizardEulaStepFooter />,
            isCurrent: currentStep === "Eula",
            isAvailable: true,
            isVisible: false,
        },
        {
            title: "Setup method",
            description: getSetupMethodDescription(setupMethod),
            component: <SetupWizardSetupMethodStep />,
            footer: <SetupWizardSetupMethodStepFooter />,
            isCurrent: currentStep === "Setup method",
            isAvailable: true,
            isVisible: getIsNotInStepIds(["Eula"]),
        },
        {
            title: "Use setup package",
            description: "Continue setup from existing package",
            component: <SetupWizardUsePackageStep />,
            footer: <SetupWizardUsePackageStepFooter />,
            isCurrent: currentStep === "Use setup package",
            isAvailable: setupMethod === "usePackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method"]),
        },
        {
            title: "License key",
            description: "Enter your license key or generate a new one",
            component: <SetupWizardLicenseKeyStep />,
            footer: <SetupWizardLicenseKeyStepFooter />,
            isCurrent: currentStep === "License key",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "Use setup package"]),
        },
        {
            title: "Security",
            description: getSecurityOptionDescription(securityOption),
            component: <SetupWizardSecurityStep />,
            footer: <SetupWizardSecurityStepFooter />,
            isCurrent: currentStep === "Security",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "Use setup package"]),
        },
        {
            title: "Self-signed certificate",
            description: "Use a self-signed certificate",
            component: <SetupWizardSelfSignedCertificateStep />,
            footer: <SetupWizardSelfSignedCertificateStepFooter />,
            isCurrent: currentStep === "Self-signed certificate",
            isAvailable:
                (setupMethod === "newCluster" || setupMethod === "createPackage") &&
                securityOption === "ownCertificate",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "Security"]),
        },
        {
            title: "Domain",
            description: "Enter your domain",
            component: <SetupWizardDomainStep />,
            footer: <SetupWizardDomainStepFooter />,
            isCurrent: currentStep === "Domain",
            isAvailable:
                (setupMethod === "newCluster" || setupMethod === "createPackage") && securityOption === "letsEncrypt",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security"]),
        },
        {
            title: "Node addresses",
            description: "Configure your cluster settings",
            component: <SetupWizardNodeAddressStep />,
            footer: <SetupWizardNodeAddressStepFooter />,
            isCurrent: currentStep === "Node addresses",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security"]),
        },
        {
            title: "Additional settings",
            description: "Customize your configuration",
            component: <SetupWizardAdditionalSettingsStep />,
            footer: <SetupWizardAdditionalSettingsStepFooter />,
            isCurrent: currentStep === "Additional settings",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security"]),
        },
        {
            title: "Summary",
            description: "Check that everything is correct",
            component: <SetupWizardSummaryStep />,
            footer: <SetupWizardSummaryStepFooter />,
            isCurrent: currentStep === "Summary",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security", "Use setup package"]),
        },
        {
            title: "Finish",
            description: finishStatus === "Completed" ? "Installation successful" : "Proceed to cluster installation",
            component: <SetupWizardFinishStep />,
            footer: <SetupWizardFinishStepFooter />,
            isCurrent: currentStep === "Finish",
            isAvailable: true,
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security"]),
        },
    ];

    return steps.filter((x) => x.isAvailable);
}

const getSetupMethodDescription = (setupMethod: SetupWizardSetupMethod) => {
    switch (setupMethod) {
        case "newCluster":
            return "Set up new cluster";
        case "createPackage":
            return "Create package for external setup";
        case "usePackage":
            return "Use setup package";
        default:
            return "Choose your setup method";
    }
};

const getSecurityOptionDescription = (securityOption: SetupWizardSecurityOption) => {
    switch (securityOption) {
        case "letsEncrypt":
            return "Generate Let's Encrypt certificate";
        case "ownCertificate":
            return "Provide your own certificate";
        case "none":
            return "Don't use certificate";
        default:
            return "Choose a security option";
    }
};
