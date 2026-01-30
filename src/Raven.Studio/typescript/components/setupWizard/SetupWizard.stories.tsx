import { withForceRerender, withBootstrap5, withStorybookContexts_DisabledSplitView } from "test/storybookTestUtils";
import SetupWizard from "./SetupWizard";
import { mockServices } from "test/mocks/services/MockServices";
import { Canvas } from "storybook/internal/types";
import {
    SetupWizardSecurityOption,
    SetupWizardSetupMethod,
    SetupWizardStepId,
} from "components/setupWizard/setupWizardValidation";
import { setupWizardConstants } from "components/setupWizard/utils/setupWizardConstants";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { expect, waitFor } from "storybook/test";
import { userEvent } from "storybook/internal/test";

const getSecurityOptionLabel = (option: SetupWizardSecurityOption): string => {
    switch (option) {
        case "none":
            return "Don't use certificate";
        case "ownCertificate":
            return "Provide your own certificate";
        case "letsEncrypt":
            return "Generate Let's Encrypt certificate";
    }
};

const getSetupMethodLabel = (option: SetupWizardSetupMethod): string => {
    switch (option) {
        case "newCluster":
            return "Set up new cluster";
        case "createPackage":
            return "Create package for external setup";
        case "usePackage":
            return "Use setup package";
    }
};

export default {
    title: "Setup Wizard",
    decorators: [withStorybookContexts_DisabledSplitView, withBootstrap5, withForceRerender],
    argTypes: {
        licenseType: {
            control: {
                type: "select",
                labels: Object.entries(setupWizardConstants.SETUP_WIZARD_MOCK_LICENSE_KEYS_IDS).reduce(
                    (acc, [key, value]) => ({
                        ...acc,
                        [value]: key,
                    }),
                    {}
                ) satisfies Partial<Record<Raven.Server.Commercial.LicenseType, string>>,
            },
            options: Object.values(setupWizardConstants.SETUP_WIZARD_MOCK_LICENSE_KEYS_IDS),
        },
        securityOption: {
            control: {
                type: "select",
                labels: {
                    letsEncrypt: "Generate Let's Encrypt certificate",
                    ownCertificate: "Provide your own certificate",
                    none: "Don't use certificate",
                } satisfies Record<SetupWizardSecurityOption, string>,
            },
            options: ["none", "letsEncrypt", "ownCertificate"] satisfies SetupWizardSecurityOption[],
        },
        setupMethod: {
            control: {
                type: "select",
                labels: {
                    newCluster: "Set up new cluster",
                    createPackage: "Create package for external setup",
                    usePackage: "Use setup package",
                } satisfies Record<SetupWizardSetupMethod, string>,
            },
            options: ["newCluster", "createPackage", "usePackage"] satisfies SetupWizardSetupMethod[],
        },
    },
    args: {
        securityOption: "letsEncrypt",
        licenseType: setupWizardConstants.SETUP_WIZARD_MOCK_LICENSE_KEYS_IDS
            .Community as Raven.Server.Commercial.LicenseType,
        setupMethod: "newCluster",
    },
} satisfies Meta<SetupWizardStoryArgs>;

interface SetupWizardStoryArgs {
    licenseType: Raven.Server.Commercial.LicenseType;
    securityOption: SetupWizardSecurityOption;
    setupMethod: SetupWizardSetupMethod;
}

export const Eula: StoryObj = {
    render: () => {
        const { setupWizardService, resourcesService, licenseService } = mockServices;

        setupWizardService.withEula();
        setupWizardService.withNodesInfoFromPackage();
        setupWizardService.withRegistrationInfo();
        setupWizardService.withHostsForCertificate();
        setupWizardService.withGetSetupLocalNodeIps();
        setupWizardService.withGetSetupParameters();
        setupWizardService.withGetIpsInfo();
        setupWizardService.withCheckDomainAvailability();
        setupWizardService.withClaimDomain();
        setupWizardService.withLetsEncryptAgreement();
        resourcesService.withFolderPathOptions_ServerLocal();
        licenseService.withVerifyLicense();

        return <SetupWizard />;
    },
};

export const SetupMethod: StoryObj = {
    ...Eula,
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "Setup method");
    },
};

export const UsePackage: StoryObj = {
    ...Eula,
    args: {
        setupMethod: "usePackage",
    },
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "Use setup package");
    },
};

export const LicenseKey: StoryObj<SetupWizardStoryArgs> = {
    ...Eula,
    name: "License key",
    play: async ({ canvas, args }) => {
        await navigateToStep(canvas, "License key", args);
    },
};

export const GenerateLicenseKey: StoryObj<SetupWizardStoryArgs> = {
    ...Eula,
    play: async ({ canvas, args }) => {
        await navigateToStep(canvas, "Generate license", args);
    },
};

export const Security: StoryObj<SetupWizardStoryArgs> = {
    ...Eula,
    play: async ({ canvas, args }) => {
        await navigateToStep(canvas, "Security", args);
    },
};

export const Domain: StoryObj<SetupWizardStoryArgs> = {
    ...Eula,
    play: async ({ canvas, args }) => {
        await navigateToStep(canvas, "Domain", args);
    },
};

export const SelfSignedCertificate: StoryObj<SetupWizardStoryArgs> = {
    ...Eula,
    name: "Self-signed certificate",
    args: {
        securityOption: "ownCertificate",
    },
    play: async ({ canvas, args }) => {
        await navigateToStep(canvas, "Self-signed certificate", args);
    },
};

export const NodeAddresses: StoryObj<SetupWizardStoryArgs> = {
    ...Eula,
    name: "Node addresses",
    play: async ({ canvas, args }) => {
        await navigateToStep(canvas, "Node addresses", args);
    },
};

export const AdditionalSettings: StoryObj<SetupWizardStoryArgs> = {
    ...Eula,
    name: "Additional settings",
    play: async ({ canvas, args }) => {
        await navigateToStep(canvas, "Additional settings", args);
    },
};

export const Summary: StoryObj<SetupWizardStoryArgs> = {
    ...Eula,
    play: async ({ canvas, args }) => {
        await navigateToStep(canvas, "Summary", args);
    },
};

export const Finish: StoryObj<SetupWizardStoryArgs> = {
    ...Eula,
    play: async ({ canvas, args }) => {
        await navigateToStep(canvas, "Finish", args);
    },
};

async function navigateToStep(
    canvas: Canvas,
    targetStep: SetupWizardStepId | "Generate license",
    args?: SetupWizardStoryArgs
): Promise<void> {
    // If target is EULA, we're already there
    if (targetStep === "Eula") {
        return;
    }

    // Accept EULA
    const eula = await canvas.findByTestId("eula-bottom");
    await waitFor(() => expect(eula).toBeInTheDocument());
    eula.scrollIntoView({ behavior: "instant" });

    let continueButton = canvas.getByRole("button", { name: /Continue/ });
    await waitFor(() => expect(continueButton).not.toBeDisabled());
    await userEvent.click(continueButton);

    // If target is SETUP_METHOD, we're done
    if (targetStep === "Setup method") {
        return;
    }

    // Handle USE_PACKAGE path
    if (targetStep === "Use setup package") {
        await userEvent.click(canvas.getByRole("heading", { name: /Use setup package/ }));
        await userEvent.click(canvas.getByRole("button", { name: /Continue/ }));
        return;
    }

    // For all other steps, go through Set up new cluster path
    await userEvent.click(canvas.getByRole("heading", { name: getSetupMethodLabel(args.setupMethod) }));
    await userEvent.click(canvas.getByRole("button", { name: /Continue/ }));

    // Enter license key
    if (args.licenseType) {
        await userEvent.clear(canvas.getByTestId("license-key-input"));
        if (args.licenseType !== "None") {
            await userEvent.type(
                canvas.getByTestId("license-key-input"),
                `{{ "Id": "${args.licenseType}", "Name": "RavenDB", "Keys": [[] }`
            );
        } else {
            return;
        }
    }

    if (targetStep === "License key") {
        return;
    }

    if (targetStep === "Generate license") {
        await userEvent.click(canvas.getByRole("button", { name: /Get your free license/ }));
        return;
    }

    // Continue to SECURITY
    continueButton = canvas.getByRole("button", { name: /Continue/ });
    await waitFor(() => expect(continueButton).not.toBeDisabled());
    await userEvent.click(continueButton);

    if (targetStep === "Security") {
        return;
    }

    // Handle certificate paths
    if (args.securityOption === "ownCertificate") {
        await userEvent.click(canvas.getByRole("heading", { name: /Provide your own certificate/ }));
        await userEvent.click(canvas.getByRole("button", { name: /Continue/ }));

        const mockedCertificateFile = new File(["foo-bar"], "certificate.pfx");
        await userEvent.upload(await canvas.findByTestId("file-input"), mockedCertificateFile);
        if (targetStep === "Self-signed certificate") {
            return;
        } else {
            await userEvent.click(canvas.getByRole("button", { name: /Continue/ }));
        }

        return;
    }

    // For the remaining steps, go through Let's Encrypt a certificate path
    await userEvent.click(canvas.getByRole("heading", { name: getSecurityOptionLabel(args.securityOption) }));
    if (args.securityOption === "letsEncrypt") {
        await userEvent.click(canvas.getByRole("checkbox"));
    }
    await userEvent.click(canvas.getByRole("button", { name: /Continue/ }));

    if (targetStep === "Domain") {
        return;
    }

    // Continue to NODE_ADDRESSES
    if (args.securityOption !== "none") {
        await userEvent.click(canvas.getByRole("button", { name: /Continue/ }));
    }

    if (targetStep === "Node addresses") {
        return;
    }

    // Save and continue to ADDITIONAL_SETTINGS
    await userEvent.click(canvas.getByRole("button", { name: /Save/ }));
    await userEvent.click(canvas.getByRole("button", { name: /Continue/ }));

    if (targetStep === "Additional settings") {
        return;
    }

    // Continue to SUMMARY
    await userEvent.click(canvas.getByRole("button", { name: /Continue/ }));

    if (targetStep === "Summary") {
        return;
    }

    // Finish the wizard
    await userEvent.click(canvas.getByRole("button", { name: /Finish/ }));
}
