import z from "zod";
import type { WizardSteps } from "@/components/form/wizard/form-wizard";
import { Badge } from "@/components/shadcn/ui/badge";
import { getOptionLabel } from "@/lib/form-utils";
import {
    appSchema,
    createExternalConnectionSchema,
    type AppFormData,
    type AppStepId,
} from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { ChooseDataSourceStep } from "@/pages/setup/add-app-wizard/steps/data-source/choose-data-source-step";
import { DATA_SOURCE_OPTIONS } from "@/pages/setup/add-app-wizard/steps/data-source/data-source-options";
import { MapSourceBadge } from "@/pages/setup/add-app-wizard/steps/map/map-source-badge";
import { ConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-step";
import { PlannerManualMappingAction } from "@/pages/setup/add-app-wizard/steps/map/planner-manual-mapping-action";
import { PlannerStep } from "@/pages/setup/add-app-wizard/steps/map/planner-step";
import { usePlannerNext } from "@/pages/setup/add-app-wizard/steps/map/use-planner-next";
import { useApplyPlanner } from "@/pages/setup/add-app-wizard/steps/map/use-apply-planner";
import { MapTablesStep } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-step";
import { PreviewStep } from "@/pages/setup/add-app-wizard/steps/preview/preview-step";
import { ExportConfigAction } from "@/pages/setup/add-app-wizard/steps/preview/export-config-action";
import { ImportConfigHeaderAction } from "@/pages/setup/add-app-wizard/steps/connect/import-config-header-action";
import { VerifySchemaStep } from "@/pages/setup/add-app-wizard/steps/verify/verify-schema-step";
import { useConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { useFocusMapTablesError } from "@/pages/setup/add-app-wizard/steps/map-tables/use-focus-map-tables-error";
import { useMapTablesStep } from "@/pages/setup/add-app-wizard/steps/map-tables/use-map-tables-step";
import { useIsVerifyCdcRunning, useVerifyCdcStep } from "@/pages/setup/add-app-wizard/steps/verify/use-verify-cdc-step";
import { usePlannerSelectionConfirm } from "@/pages/setup/add-app-wizard/steps/verify/use-planner-selection-confirm";
import { VerifySelectionChangedBadge } from "@/pages/setup/add-app-wizard/steps/verify/verify-selection-changed-badge";

export const useAppSteps = (): WizardSteps<AppStepId, AppFormData> => {
    const connectSourceBeforeNext = useConnectSourceStep();
    const verifyCdcBeforeNext = useVerifyCdcStep();
    const isVerifyCdcRunning = useIsVerifyCdcRunning();
    const plannerSelectionConfirm = usePlannerSelectionConfirm();
    const plannerNext = usePlannerNext();
    const applyPlanner = useApplyPlanner();
    const mapTablesBeforeNext = useMapTablesStep();
    const focusMapTablesError = useFocusMapTablesError();

    return {
        dataSource: {
            title: "Choose data source",
            description: "Where is the data this app will work with?",
            bodyComponent: ChooseDataSourceStep,
            validate: "dataSource",
            badgeFields: ["dataSource.source"],
            badge: ({ isComplete, values }) => {
                if (!isComplete) {
                    return null;
                }
                return <Badge variant="primary">{getOptionLabel(DATA_SOURCE_OPTIONS, values.dataSource.source)}</Badge>;
            },
        },
        externalConnection: {
            title: "Connect to your source database",
            bodyComponent: ConnectSourceStep,
            headerAction: ImportConfigHeaderAction,
            validate: "externalConnection",
            beforeNext: connectSourceBeforeNext,
            badgeFields: [],
            badge: ({ isComplete }) => {
                if (!isComplete) {
                    return null;
                }
                return <Badge variant="success">Successfully connected</Badge>;
            },
        },
        verifySchema: {
            title: "Verify your schema",
            description:
                "Choose the tables this app will work with. Only the selected tables are carried into the " +
                "mapping and the task configuration - unselected tables, and relationships pointing to them, are " +
                "ignored. Clicking Next runs a configuration validation dry run against the source database.",
            bodyComponent: VerifySchemaStep,
            isFullHeight: true,
            validate: "verifySchema",
            beforeNext: verifyCdcBeforeNext,
            confirmNext: plannerSelectionConfirm,
            // Advancing mid-run would carry a selection the dry run has not answered for yet.
            isNextDisabled: isVerifyCdcRunning,
            badgeFields: ["verifySchema.tables"],
            badge: ({ values }) => <VerifySelectionChangedBadge tables={values.verifySchema.tables} />,
        },
        map: {
            title: "Plan your document model",
            bodyComponent: PlannerStep,
            headerAction: PlannerManualMappingAction,
            isFullHeight: true,
            // The mapping is produced by the planner, not by form fields, so there is nothing here
            // for the resolver to check. Applying it is what Next does.
            validate: false,
            nextLabel: plannerNext.label,
            beforeNext: applyPlanner,
            isNextDisabled: plannerNext.isNextDisabled,
            nextNote: plannerNext.note,
            badge: ({ isComplete }) => (isComplete ? <MapSourceBadge /> : null),
        },
        mapTables: {
            title: "Map schema",
            description: "Configure how source tables are mapped to target collections.",
            bodyComponent: MapTablesStep,
            isFullHeight: true,
            validate: "mapTables",
            onValidationFailed: focusMapTablesError,
            beforeNext: mapTablesBeforeNext,
            isNextDisabled: isVerifyCdcRunning,
        },
        preview: {
            title: "Preview before full ingest",
            description:
                "Ingest chosen number of rows per root table into a throwaway namespace so you can check shape before the real load.",
            bodyComponent: PreviewStep,
            footerComponent: ExportConfigAction,
            validate: "preview",
        },
    };
};

export const getAppFlow = ({ dataSource, isEditing }: { dataSource: string; isEditing?: boolean }): AppStepId[] => {
    // The "Choose data source" step is temporarily dropped from the UI; uncomment the
    // "dataSource" entries below to bring it back.
    if (dataSource === "ravendb") {
        return [/* "dataSource", */ "preview"];
    }

    // An edited app arrives with its stored mapping, so it goes straight to the editor.
    if (isEditing) {
        return ["externalConnection", "verifySchema", "mapTables", "preview"];
    }

    return [/* "dataSource", */ "externalConnection", "verifySchema", "map", "mapTables", "preview"];
};

export const buildAppSchemaForFlow = (flow: AppStepId[], takenSlugs: string[] = []) => {
    const schemaStepIds = Object.keys(appSchema.shape) as AppStepId[];
    const skippedSchemaSteps = schemaStepIds.filter((stepId) => !flow.includes(stepId));

    const overrides: Record<string, z.ZodTypeAny> = Object.fromEntries(
        skippedSchemaSteps.map((stepId) => [stepId, z.any()]),
    );

    if (takenSlugs.length > 0 && !skippedSchemaSteps.includes("externalConnection")) {
        overrides.externalConnection = createExternalConnectionSchema(takenSlugs);
    }

    if (Object.keys(overrides).length === 0) {
        return appSchema;
    }

    return appSchema.extend(overrides) as typeof appSchema;
};
