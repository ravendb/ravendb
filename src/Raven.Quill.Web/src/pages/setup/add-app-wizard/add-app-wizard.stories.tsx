import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm } from "react-hook-form";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { preventEnterKeySubmission } from "@/lib/form-utils";
import { defaultApiMocks } from "@/mocks/default-mocks";
import { discoveryWithAllStates, failedDiscovery, sampleDiscovery, setupMocks } from "@/mocks/setup-mocks";
import { AddAppWizard } from "./add-app-wizard";
import { getAppFlow, useAppSteps } from "./app-wizard-flow";
import { useSetupWizardStore } from "./app-wizard-store";
import { isTableSupported } from "./discover-utils";
import { appSchema, type AppFormData, type AppStepId } from "./app-wizard-validation";
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
            connectionString: "Host=localhost;Port=5432;Database=acme_shop;Username=admin;Password=secret",
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
}: {
    initialStep: AppStepId;
    discovery?: DiscoverResponse;
}) {
    useState(() =>
        useSetupWizardStore.setState({
            discoverResult: discovery,
            discoverSchemas: [],
            importState: "none",
            connectKey: null,
            appliedMapKey: null,
            mapTablesKey: null,
            mapActiveTable: { type: "root", path: "mapTables.tables.0" },
            mapExpandedPaths: {},
        }),
    );

    const form = useForm<AppFormData>({
        mode: "onChange",
        defaultValues: buildSeed(discovery),
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
            completion={{ type: "submit", label: "Create app & continue" }}
        />
    );
}

export const ChooseDataSource: Story = {
    render: () => <AppWizardAtStep initialStep="dataSource" />,
};

export const ConnectSource: Story = {
    render: () => <AppWizardAtStep initialStep="externalConnection" />,
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

export const MapSchema: Story = {
    render: () => <AppWizardAtStep initialStep="map" />,
};

export const MapTables: Story = {
    render: () => <AppWizardAtStep initialStep="mapTables" />,
};

export const Preview: Story = {
    render: () => <AppWizardAtStep initialStep="preview" />,
};
