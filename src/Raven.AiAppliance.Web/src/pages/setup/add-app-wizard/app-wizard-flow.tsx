import z from "zod";
import type { WizardSteps } from "@/components/form/wizard/form-wizard";
import { Badge } from "@/components/shadcn/ui/badge";
import { appSchema, type AppFormData, type AppStepId } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { ChooseDataSourceStep } from "@/pages/setup/add-app-wizard/steps/data-source/choose-data-source-step";
import { ConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-step";
import { MapAiSuggestStep } from "@/pages/setup/add-app-wizard/steps/map-ai-suggested/map-ai-suggest-step";
import { MapSchemaStep } from "@/pages/setup/add-app-wizard/steps/map/map-schema-step";
import { PreviewStep } from "@/pages/setup/add-app-wizard/steps/preview/preview-step";
import { VerifySchemaStep } from "@/pages/setup/add-app-wizard/steps/verify/verify-schema-step";
import { useConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { useMapAiSuggestStep } from "@/pages/setup/add-app-wizard/steps/map-ai-suggested/use-map-ai-suggest-step";
import { useMapSchemaStep } from "@/pages/setup/add-app-wizard/steps/map/use-map-schema-step";

export const useAppSteps = (): WizardSteps<AppStepId, AppFormData> => {
    const connectSourceStep = useConnectSourceStep();
    const mapSchemaStep = useMapSchemaStep();
    const mapAiSuggestStep = useMapAiSuggestStep();

    return {
        dataSource: {
            title: "Choose data source",
            description: "Where is the data this application will work with?",
            bodyComponent: ChooseDataSourceStep,
            validate: "dataSource",
            badgeFields: ["dataSource.source"],
            badge: ({ values }) => (
                <Badge variant="secondary">
                    {values.dataSource?.source === "ravendb" ? "RavenDB database" : "External database"}
                </Badge>
            ),
        },
        externalConnection: {
            title: "Connect to your source database",
            bodyComponent: ConnectSourceStep,
            validate: "externalConnection",
            beforeNext: connectSourceStep.mutateAsync,
            isPending: connectSourceStep.isPending,
            error: connectSourceStep.error,
            badgeFields: [],
            badge: ({ isComplete }) =>
                isComplete ? (
                    <Badge className="border-transparent bg-emerald-500/15 text-emerald-700 dark:text-emerald-400">
                        Successfully connected
                    </Badge>
                ) : null,
        },
        verifySchema: {
            title: "Verify your schema",
            description: "Fetch existing tables from the linked source.",
            bodyComponent: VerifySchemaStep,
            validate: "verifySchema",
        },
        map: {
            title: "How would you like to map your schema?",
            bodyComponent: MapSchemaStep,
            validate: "map",
            beforeNext: mapSchemaStep.mutateAsync,
            isPending: mapSchemaStep.isPending,
            error: mapSchemaStep.error,
            badgeFields: ["map.source"],
            badge: ({ values }) => (
                <Badge variant="secondary">{values.map?.source === "manual" ? "Manual" : "Map with AI"}</Badge>
            ),
        },
        mapAiSuggest: {
            title: "Map schema",
            description: "Review the draft mapping the AI proposed from your intent and the discovered schema.",
            bodyComponent: MapAiSuggestStep,
            validate: "mapAiSuggest",
            beforeNext: mapAiSuggestStep.mutateAsync,
            isPending: mapAiSuggestStep.isPending,
            error: mapAiSuggestStep.error,
        },
        mapManual: {
            title: "Map schema",
            bodyComponent: () => <div>Map schema manual</div>,
            validate: "mapManual",
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

export const getAppFlow = ({ dataSource, mapSource }: { dataSource: string; mapSource: string }): AppStepId[] => {
    if (dataSource === "ravendb") {
        return ["dataSource", "preview"];
    }

    return [
        "dataSource",
        "externalConnection",
        "verifySchema",
        "map",
        mapSource === "ai-suggested" ? "mapAiSuggest" : "mapManual",
        "preview",
    ];
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
