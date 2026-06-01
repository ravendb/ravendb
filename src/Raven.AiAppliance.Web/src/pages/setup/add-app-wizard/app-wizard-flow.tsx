import z from "zod";
import type { WizardSteps } from "@/components/form/wizard/form-wizard";
import { appSchema, type AppStepId } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { ChooseDataSourceStep } from "@/pages/setup/add-app-wizard/steps/data-source/choose-data-source-step";
import { ConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-step";
import { MapAiSuggestStep } from "@/pages/setup/add-app-wizard/steps/map-ai-suggested/map-ai-suggest-step";
import { MapSchemaStep } from "@/pages/setup/add-app-wizard/steps/map/map-schema-step";
import { PreviewStep } from "@/pages/setup/add-app-wizard/steps/preview/preview-step";
import { VerifySchemaStep } from "@/pages/setup/add-app-wizard/steps/verify/verify-schema-step";
import { useMapSchemaStep } from "@/pages/setup/add-app-wizard/steps/map/use-map-schema-step";
import { useConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";
import { useMapAiSuggestStep } from "@/pages/setup/add-app-wizard/steps/map-ai-suggested/use-map-ai-suggest-step";

export const useAppSteps = (): WizardSteps<AppStepId> => {
    const connectSourceStep = useConnectSourceStep();
    const mapSchemaStep = useMapSchemaStep();
    const mapAiSuggestStep = useMapAiSuggestStep();

    return {
        dataSource: {
            id: "dataSource",
            title: "Choose data source",
            description: "Where is the data this application will work with?",
            bodyComponent: ChooseDataSourceStep,
        },
        externalConnection: {
            id: "externalConnection",
            title: "Connect to your source database",
            bodyComponent: ConnectSourceStep,
            beforeNext: connectSourceStep.mutateAsync,
            status: connectSourceStep.status,
            error: connectSourceStep.error,
        },
        verifySchema: {
            id: "verifySchema",
            title: "Verify your schema",
            description: "Fetch existing tables from the linked source.",
            bodyComponent: VerifySchemaStep,
        },
        map: {
            id: "map",
            title: "How would you like to map your schema?",
            bodyComponent: MapSchemaStep,
            beforeNext: mapSchemaStep.mutateAsync,
            status: mapSchemaStep.status,
            error: mapSchemaStep.error,
        },
        mapAiSuggest: {
            id: "mapAiSuggest",
            title: "Map schema",
            description: "Review the draft mapping the AI proposed from your intent and the discovered schema.",
            bodyComponent: MapAiSuggestStep,
            beforeNext: mapAiSuggestStep.mutateAsync,
            status: mapAiSuggestStep.status,
            error: mapAiSuggestStep.error,
        },
        mapManual: {
            id: "mapManual",
            title: "Map schema",
            bodyComponent: () => <div>Map schema manual</div>,
        },
        preview: {
            id: "preview",
            title: "Preview before full ingest",
            description:
                "Ingest chosen number of rows per root table into a throwaway namespace so you can check shape before the real load.",
            bodyComponent: PreviewStep,
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
