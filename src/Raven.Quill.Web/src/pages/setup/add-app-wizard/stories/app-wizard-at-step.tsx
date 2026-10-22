import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm } from "react-hook-form";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { preventEnterKeySubmission } from "@/lib/form-utils";
import { sampleDiscovery } from "@/mocks/setup-mocks";
import { getAppFlow, useAppSteps } from "@/pages/setup/add-app-wizard/app-wizard-flow";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { appSchema, type AppFormData, type AppStepId } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { buildSeed } from "@/pages/setup/add-app-wizard/stories/app-wizard-fixtures";

// Renders the real wizard jumped to a single step. The body components read the discovery
// result and map-tables selection from the store, so seed both before they first render
// (the Full Wizard story's AddAppWizard resets the store on mount, so this never leaks).
export function AppWizardAtStep({
    initialStep,
    discovery = sampleDiscovery,
    hasSelectedTables = true,
    seedOverride,
    seedStore,
}: {
    initialStep: AppStepId;
    discovery?: DiscoverResponse;
    /** When false, the verify step starts with nothing selected, as it does on a fresh discovery. */
    hasSelectedTables?: boolean;
    seedOverride?: (seed: AppFormData) => AppFormData;
    /** Runs once the store is seeded, to bring it to the state the story starts from. */
    seedStore?: (seed: AppFormData) => void;
}) {
    const [seed] = useState(() => {
        const built = buildSeed(discovery);
        const values = hasSelectedTables ? built : { ...built, verifySchema: { tables: [] } };

        return seedOverride ? seedOverride(values) : values;
    });

    useState(() => {
        useSetupWizardStore.getState().resetPlannerState();
        useSetupWizardStore.setState({
            discoverResult: discovery,
            discoverSchemas: [],
            editedAppSlug: null,
            initialSelectedTables: null,
            connectKey: null,
            // The seeded mapping is the manual scaffold.
            mapOrigin: seed.mapTables.tables.length > 0 ? { source: "manual" } : null,
            mapTablesKey: null,
            mapActiveTable: { type: "root", path: "mapTables.tables.0" },
            mapExpandedPaths: {},
        });
        seedStore?.(seed);
    });

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
