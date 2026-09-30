import type { Meta, StoryObj } from "@storybook/react-vite";
import { sampleCertificates, settingsMocks } from "@/mocks/settings-mocks";
import { DashboardCertificates } from "./certificates";

const meta = {
    title: "Dashboard/Certificates",
    component: DashboardCertificates,
    parameters: {
        // The page renders its own "Certificates" header with the refresh and
        // generate buttons beside it, so the shell decorator adds no title.
        page: {},
    },
} satisfies Meta<typeof DashboardCertificates>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        msw: {
            handlers: {
                settings: [
                    settingsMocks.feedback(),
                    settingsMocks.license(),
                    settingsMocks.usage(),
                    settingsMocks.certificates([]),
                    settingsMocks.certificatesGenerate(),
                    settingsMocks.certificatesEdit(),
                ],
            },
        },
    },
};

// Open "Generate client certificate" and submit to see the server error surfaced
// inside the dialog.
export const GenerateError: Story = {
    parameters: {
        msw: {
            handlers: {
                settings: [
                    settingsMocks.feedback(),
                    settingsMocks.license(),
                    settingsMocks.usage(),
                    settingsMocks.certificates(sampleCertificates),
                    settingsMocks.certificatesGenerateError(),
                    settingsMocks.certificatesEdit(),
                ],
            },
        },
    },
};
