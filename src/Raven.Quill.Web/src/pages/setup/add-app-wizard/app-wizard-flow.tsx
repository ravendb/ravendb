import z from "zod";
import type { WizardSteps } from "@/components/form/wizard/form-wizard";
import { Badge } from "@/components/shadcn/ui/badge";
import { getOptionLabel } from "@/lib/form-utils";
import { appSchema, type AppFormData, type AppStepId } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { ChooseDataSourceStep } from "@/pages/setup/add-app-wizard/steps/data-source/choose-data-source-step";
import { DATA_SOURCE_OPTIONS } from "@/pages/setup/add-app-wizard/steps/data-source/data-source-options";
import { MAP_SOURCE_OPTIONS } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";
import { ConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-step";
import { MapSchemaStep } from "@/pages/setup/add-app-wizard/steps/map/map-schema-step";
import { MapTablesStep } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-step";
import { PreviewStep } from "@/pages/setup/add-app-wizard/steps/preview/preview-step";
import { VerifySchemaStep } from "@/pages/setup/add-app-wizard/steps/verify/verify-schema-step";
import { useConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { useMapSchemaStep } from "@/pages/setup/add-app-wizard/steps/map/use-map-schema-step";
import { useFocusMapTablesError } from "@/pages/setup/add-app-wizard/steps/map-tables/use-focus-map-tables-error";
import { useMapTablesStep } from "@/pages/setup/add-app-wizard/steps/map-tables/use-map-tables-step";

export const useAppSteps = (): WizardSteps<AppStepId, AppFormData> => {
    const connectSourceBeforeNext = useConnectSourceStep();
    const mapSchemaBeforeNext = useMapSchemaStep();
    const mapTablesBeforeNext = useMapTablesStep();
    const focusMapTablesError = useFocusMapTablesError();

    return {
        dataSource: {
            title: "Choose data source",
            description: "Where is the data this application will work with?",
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
            description: "Fetch existing tables from the linked source.",
            bodyComponent: VerifySchemaStep,
            isFullHeight: true,
            validate: "verifySchema",
        },
        map: {
            title: "How would you like to map your schema?",
            bodyComponent: MapSchemaStep,
            validate: "map",
            beforeNext: mapSchemaBeforeNext,
            badgeFields: ["map.source"],
            badge: ({ isComplete, values }) => {
                if (!isComplete) {
                    return null;
                }

                return <Badge variant="primary">{getOptionLabel(MAP_SOURCE_OPTIONS, values.map?.source)}</Badge>;
            },
        },
        mapTables: {
            title: "Map schema",
            description: "Configure how source tables are mapped to target collections.",
            bodyComponent: MapTablesStep,
            isFullHeight: true,
            validate: "mapTables",
            onValidationFailed: focusMapTablesError,
            beforeNext: mapTablesBeforeNext,
        },
        preview: {
            title: "Preview before full ingest",
            description:
                "Ingest chosen number of rows per root table into a throwaway namespace so you can check shape before the real load.",
            bodyComponent: PreviewStep,
            validate: "preview",
        },
    };
};

export const getAppFlow = ({ dataSource }: { dataSource: string }): AppStepId[] => {
    // The "Choose data source" step is temporarily dropped from the UI; uncomment the
    // "dataSource" entries below to bring it back.
    if (dataSource === "ravendb") {
        return [/* "dataSource", */ "preview"];
    }

    return [/* "dataSource", */ "externalConnection", "verifySchema", "map", "mapTables", "preview"];
};

export const buildAppSchemaForFlow = (flow: AppStepId[]) => {
    const schemaStepIds = Object.keys(appSchema.shape) as AppStepId[];
    const skippedSchemaSteps = schemaStepIds.filter((stepId) => !flow.includes(stepId));

    if (skippedSchemaSteps.length === 0) {
        return appSchema;
    }

    return appSchema.extend(
        Object.fromEntries(skippedSchemaSteps.map((stepId) => [stepId, z.any()])) as Record<string, z.ZodTypeAny>,
    ) as typeof appSchema;
};
