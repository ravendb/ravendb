import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm } from "react-hook-form";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { RANGE_PREVIEW_ROW_CLASSNAME } from "@/components/table/row-range-selection";
import { preventEnterKeySubmission } from "@/lib/form-utils";
import { defaultApiMocks } from "@/mocks/default-mocks";
import {
    discoveryWithAllStates,
    failedCdcVerification,
    failedDiscovery,
    manyTablesDiscovery,
    sampleDiscovery,
    setupMocks,
} from "@/mocks/setup-mocks";
import { AddAppWizard } from "./add-app-wizard";
import { getAppFlow, useAppSteps } from "./app-wizard-flow";
import { useSetupWizardStore } from "./app-wizard-store";
import { isTableSupported } from "./discover-utils";
import { appSchema, type AppFormData, type AppStepId } from "./app-wizard-validation";
import { computeSourceKey } from "./steps/connect/use-connect-source-step";
import { computeMapKey } from "./steps/map/use-map-schema-step";
import { scaffoldRootTable } from "./steps/map-tables/map-tables-utils";

const meta = {
    title: "Setup/Add App Wizard",
    component: AddAppWizard,
    parameters: {
        page: { bare: true },
    },
} satisfies Meta<typeof AddAppWizard>;

export default meta;

type Story = StoryObj<typeof meta>;

// The full wizard, mounted at the first step through the real entry component (form
// creation, store reset, and provision mutation wired up).
export const Default: Story = {};

// Valid values for every step, so any step renders with realistic data and Next/Back keep
// working against the default MSW mocks. The verify step's initial selection follows the
// discovery being shown; map/preview always seed from the clean happy-path discovery.
function buildSeed(discovery: DiscoverResponse): AppFormData {
    return {
        dataSource: { source: "external" },
        externalConnection: {
            appName: "AcmeShop",
            slug: "acme-shop",
            provider: "Npgsql",
            mode: "fields",
            fields: {
                host: "localhost",
                port: 5432,
                database: "acme_shop",
                username: "admin",
                password: "secret",
                isSecured: false,
            },
            connectionString: "",
        },
        verifySchema: {
            tables: discovery.tables
                .filter((table) => isTableSupported(discovery, table))
                .map((table) => ({
                    sourceTableSchema: table.sourceTableSchema,
                    sourceTableName: table.sourceTableName,
                })),
        },
        map: { source: "ai-suggested", aiPrompt: "Embed order line items and link customers by id." },
        mapTables: { tables: sampleDiscovery.tables.map((table) => scaffoldRootTable(sampleDiscovery, table)) },
        preview: { table: "dbo.Customers", maxRows: 1 },
    };
}

// Replaces the discover handler so re-discovery ("Customize schemas") stays consistent
// with the seeded result, keeping the rest of the setup mocks intact.
function discoverHandlers(discovery: DiscoverResponse) {
    return { setup: [setupMocks.discover(discovery), ...defaultApiMocks.setup] };
}

// Renders the real wizard jumped to a single step. The body components read the discovery
// result and map-tables selection from the store, so seed both before they first render
// (Default's AddAppWizard resets the store on mount, so this never leaks).
function AppWizardAtStep({
    initialStep,
    discovery = sampleDiscovery,
    isMappingApplied = true,
    hasSelectedTables = true,
    seedOverride,
}: {
    initialStep: AppStepId;
    discovery?: DiscoverResponse;
    /** When false, the map-tables step treats its seeded tables as stale and asks the AI again. */
    isMappingApplied?: boolean;
    /** When false, the verify step starts with nothing selected, as it does on a fresh discovery. */
    hasSelectedTables?: boolean;
    seedOverride?: (seed: AppFormData) => AppFormData;
}) {
    const [seed] = useState(() => {
        const built = buildSeed(discovery);
        const values = hasSelectedTables ? built : { ...built, verifySchema: { tables: [] } };

        return seedOverride ? seedOverride(values) : values;
    });

    useState(() =>
        useSetupWizardStore.setState({
            discoverResult: discovery,
            discoverSchemas: [],
            importState: "none",
            connectKey: null,
            appliedMapKey: isMappingApplied
                ? computeMapKey({
                      sourceKey: computeSourceKey(seed.externalConnection),
                      source: seed.map.source,
                      aiPrompt: seed.map.aiPrompt,
                      selectedTables: seed.verifySchema.tables,
                  })
                : null,
            mapTablesKey: null,
            mapActiveTable: { type: "root", path: "mapTables.tables.0" },
            mapExpandedPaths: {},
        }),
    );

    const form = useForm<AppFormData>({
        mode: "onChange",
        defaultValues: seed,
        resolver: zodResolver(appSchema),
    });

    return (
        <FormProvider {...form}>
            <form className="h-full" onSubmit={(event) => event.preventDefault()} onKeyDown={preventEnterKeySubmission}>
                <AppWizardStepBody initialStep={initialStep} />
            </form>
        </FormProvider>
    );
}

// useAppSteps reads the form via context, so it must run inside the provider above.
function AppWizardStepBody({ initialStep }: { initialStep: AppStepId }) {
    const steps = useAppSteps();

    return (
        <FormWizard
            steps={steps}
            flow={getAppFlow({ dataSource: "external" })}
            initialStep={initialStep}
            cancel={() => {}}
            completion={{ type: "submit", label: "Create app & continue", busyLabel: "Creating app..." }}
        />
    );
}

export const ChooseDataSource: Story = {
    render: () => <AppWizardAtStep initialStep="dataSource" />,
};

export const ConnectSource: Story = {
    render: () => <AppWizardAtStep initialStep="externalConnection" />,
};

export const ConnectSourceConnectionString: Story = {
    render: () => (
        <AppWizardAtStep
            initialStep="externalConnection"
            seedOverride={(seed) => ({
                ...seed,
                externalConnection: {
                    ...seed.externalConnection,
                    mode: "raw",
                    connectionString: "Host=localhost;Port=5432;Database=acme_shop;Username=admin;Password=secret",
                },
            })}
        />
    ),
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

export const ConnectSourceError: Story = {
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

// Verified tables (one with table-level warnings), tables that need configuration (CDC
// disabled and an unsupported reason), and a response-level warning banner.
export const VerifySchema: Story = {
    parameters: { msw: { handlers: discoverHandlers(discoveryWithAllStates) } },
    render: () => <AppWizardAtStep initialStep="verifySchema" discovery={discoveryWithAllStates} />,
};

// Discovery failed: only the destructive error banner is shown, no tables.
export const VerifySchemaDiscoveryFailed: Story = {
    parameters: { msw: { handlers: discoverHandlers(failedDiscovery) } },
    render: () => <AppWizardAtStep initialStep="verifySchema" discovery={failedDiscovery} />,
};

// The array-level "at least one table" error must clear as soon as a table is selected.
export const VerifySchemaWithoutSelection: Story = {
    render: () => <AppWizardAtStep initialStep="verifySchema" hasSelectedTables={false} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: /next/i }));
        await waitFor(() => expect(canvas.getByText("At least one table is required")).toBeInTheDocument());

        await userEvent.click(canvas.getAllByRole("checkbox", { name: "Select row" })[0]);
        await waitFor(() => expect(canvas.queryByText("At least one table is required")).not.toBeInTheDocument());
    },
};

export const VerifySchemaSelectionLimit: Story = {
    parameters: { msw: { handlers: discoverHandlers(manyTablesDiscovery) } },
    render: () => (
        <AppWizardAtStep initialStep="verifySchema" discovery={manyTablesDiscovery} hasSelectedTables={false} />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        // One instance for the whole run: the direct userEvent API starts a fresh one per call,
        // which would drop the held shift key before the range click.
        const user = userEvent.setup();
        const rowCheckboxes = () => canvas.getAllByRole("checkbox", { name: "Select row" });
        const limitNotice = () => canvas.queryByText(/one app processes at most 64 tables/i);
        const previewedRows = () => canvasElement.querySelectorAll(`.${RANGE_PREVIEW_ROW_CLASSNAME}`);

        expect(canvas.getByText(/while clicking to select a range of tables/i)).toBeInTheDocument();

        await user.click(rowCheckboxes()[0]);

        // Holding shift previews the range from the anchor to the hovered row before taking it.
        await user.keyboard("{Shift>}");
        await user.hover(rowCheckboxes()[3]);
        await waitFor(() => expect(previewedRows()).toHaveLength(4));

        await user.click(rowCheckboxes()[3]);
        await user.keyboard("{/Shift}");
        await waitFor(() => expect(canvas.getByText(/4 out of 80 tables selected/)).toBeInTheDocument());
        await waitFor(() => expect(previewedRows()).toHaveLength(0));
        expect(limitNotice()).not.toBeInTheDocument();

        const selectAll = canvas.getByRole("checkbox", { name: "Select all" });
        await user.click(selectAll);
        await waitFor(() => expect(canvas.getByText(/64 out of 80 tables selected/)).toBeInTheDocument());
        expect(limitNotice()).toBeInTheDocument();

        await user.click(selectAll);
        await waitFor(() => expect(limitNotice()).not.toBeInTheDocument());
    },
};

export const VerifySchemaCdcVerificationFailed: Story = {
    parameters: {
        msw: { handlers: { setup: [setupMocks.verifyCdc(failedCdcVerification), ...defaultApiMocks.setup] } },
    },
    render: () => <AppWizardAtStep initialStep="verifySchema" />,
    // The dry run's blockers keep the wizard on this step and stay on screen until the selection changes.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const findAlert = () => canvas.queryByText(/must have the REPLICATION role attribute/i);

        await userEvent.click(canvas.getByRole("button", { name: /next/i }));
        await waitFor(() => expect(findAlert()).toBeInTheDocument());
        expect(canvas.getByRole("heading", { name: /verify your schema/i })).toBeInTheDocument();

        await userEvent.click(canvas.getAllByRole("checkbox", { name: "Select row" })[0]);
        await waitFor(() => expect(findAlert()).not.toBeInTheDocument());
    },
};

export const MapSchema: Story = {
    render: () => <AppWizardAtStep initialStep="map" />,
};

export const MapTables: Story = {
    render: () => <AppWizardAtStep initialStep="mapTables" />,
};

// The suggestion call routinely runs for more than a minute, so it is parked here to keep the
// progress skeleton and its stage labels on screen.
export const MapTablesSuggesting: Story = {
    parameters: { msw: { handlers: { setup: [setupMocks.suggestCdcPending(), ...defaultApiMocks.setup] } } },
    render: () => <AppWizardAtStep initialStep="mapTables" isMappingApplied={false} />,
};

export const Preview: Story = {
    render: () => <AppWizardAtStep initialStep="preview" />,
};
