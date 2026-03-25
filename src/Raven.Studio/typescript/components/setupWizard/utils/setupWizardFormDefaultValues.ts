import { SetupWizardFormData } from "components/setupWizard/setupWizardValidation";

export const setupWizardFormDefaultValues: SetupWizardFormData = {
    currentStep: "Eula",
    setupMethodStep: {
        method: null,
    },
    usePackageStep: {
        fileZip: "",
        nodeTag: "",
        isZipValid: false,
        isZipSecure: false,
        publicServerUrl: "",
        serverUrl: "",
    },
    licenseKeyStep: {
        isAcceptTerms: false,
        isAcceptEmails: false,
        key: "",
        licenseInfo: null,
        licenseTypeToGenerate: null,
        licenseStatus: null,
        firstName: "",
        lastName: "",
        email: "",
        phone: "",
    },
    domainStep: {
        domain: "",
        email: "",
    },
    securityStep: {
        securityOption: null,
    },
    selfSignedCertificateStep: {
        certificateFileName: "",
        certificate: "",
        password: "",
        cns: [],
    },
    nodeAddressStep: {
        nodes: [],
    },
    additionalSettingsStep: {
        isAdvancedSettingsVisible: false,
        dataDirectory: null,
        setupCertificatePath: null,
        adminCertificateExpirationTime: 60,
        postgresqlIntegration: false,
        studioEnvironment: "None",
    },
    finishStep: {
        finishingStatus: "InProgress",
    },
};
