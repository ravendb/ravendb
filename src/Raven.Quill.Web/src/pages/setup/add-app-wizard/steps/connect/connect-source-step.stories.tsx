import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { defaultApiMocks } from "@/mocks/default-mocks";
import { setupMocks } from "@/mocks/setup-mocks";
import { AppWizardAtStep } from "@/pages/setup/add-app-wizard/stories/app-wizard-at-step";

const meta = {
    title: "Setup/Add App Wizard/Connect Source",
    parameters: {
        page: { bare: true },
    },
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {
    render: () => <AppWizardAtStep initialStep="externalConnection" />,
    // Switching the database type applies its default port and leaves the other details untouched.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: /sql server/i }));

        expect(canvas.getByLabelText(/port/i)).toHaveValue(1433);
        expect(canvas.getByLabelText(/host/i)).toHaveValue("localhost");
    },
};

const SEEDED_CONNECTION_STRING = "Host=localhost;Port=5432;Database=acme_shop;Username=admin;Password=secret";

export const ConnectionString: Story = {
    render: () => (
        <AppWizardAtStep
            initialStep="externalConnection"
            seedOverride={(seed) => ({
                ...seed,
                externalConnection: {
                    ...seed.externalConnection,
                    mode: "raw",
                    connectionString: SEEDED_CONNECTION_STRING,
                },
            })}
        />
    ),
    // Switching the database type leaves the pasted connection string untouched.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: /mysql/i }));

        expect(canvas.getByRole("textbox", { name: /connection string/i })).toHaveValue(SEEDED_CONNECTION_STRING);
    },
};

// Import is a header action next to the step title, and it no longer waits for an app name:
// the wizard endpoints get a draft slug until the operator provides a real one.
export const ImportConfiguration: Story = {
    render: () => (
        <AppWizardAtStep
            initialStep="externalConnection"
            seedOverride={(seed) => ({
                ...seed,
                externalConnection: { ...seed.externalConnection, appName: "", slug: "" },
            })}
        />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const heading = canvas.getByRole("heading", { name: /connect to your source database/i });
        const importButton = canvas.getByRole("button", { name: /import configuration/i });

        expect(canvas.getByLabelText(/app name/i)).toHaveValue("");
        expect(importButton).toBeEnabled();

        expect(heading.closest("header")).toContainElement(importButton);
    },
};

const connectFailureHandlers = {
    setup: [
        setupMocks.connect({
            success: false,
            errors: [
                {
                    message:
                        "Could not connect to the source database. Check that the host and port are reachable, " +
                        "the database name is correct, and the credentials are valid. " +
                        '28P01: password authentication failed for user "admin"',
                    details:
                        'Npgsql.NpgsqlException (0x80004005): 28P01: password authentication failed for user "admin"\n' +
                        "   at Npgsql.Internal.NpgsqlConnector.<Authenticate>d__0.MoveNext()\n" +
                        "   at Npgsql.Internal.NpgsqlConnector.<Open>d__1.MoveNext()\n" +
                        "   at Npgsql.NpgsqlConnection.<OpenAsync>d__2.MoveNext()",
                },
            ],
        }),
        ...defaultApiMocks.setup,
    ],
};

export const ConnectionError: Story = {
    parameters: { msw: { handlers: connectFailureHandlers } },
    render: () => <AppWizardAtStep initialStep="externalConnection" />,
    // A failure belongs to the connection it was made with: editing any part of that connection
    // drops it, and exactly one alert shows at a time.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const findAlerts = () => canvas.queryAllByText(/could not connect to the source database/i);

        await userEvent.click(canvas.getByRole("button", { name: /test connection/i }));
        await waitFor(() => expect(findAlerts()).toHaveLength(1));

        await userEvent.click(canvas.getByRole("button", { name: /sql server/i }));
        await waitFor(() => expect(findAlerts()).toHaveLength(0));

        await userEvent.click(canvas.getByRole("button", { name: /test connection/i }));
        await waitFor(() => expect(findAlerts()).toHaveLength(1));

        // Back on the provider that failed first, the alert stays gone and nothing claims success.
        await userEvent.click(canvas.getByRole("button", { name: /postgresql/i }));
        await waitFor(() => expect(findAlerts()).toHaveLength(0));
        expect(canvas.getByRole("button", { name: /test connection/i })).toBeEnabled();
    },
};
