import type { WizardSteps } from "@/components/form/wizard/form-wizard";
import type { AppStepId, AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { ChooseDataSourceStep } from "@/pages/setup/add-app-wizard/steps/choose-data-source-step";
import { useConnectSourceStep, ConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect-source-step";
import { MapAiSuggestStep } from "@/pages/setup/add-app-wizard/steps/map-ai-suggest-step";
import { useMapSchemaStep, MapSchemaStep } from "@/pages/setup/add-app-wizard/steps/map-schema-step";
import { PreviewStep } from "@/pages/setup/add-app-wizard/steps/preview-step";
import { VerifySchemaStep } from "@/pages/setup/add-app-wizard/steps/verify-schema-step";
import { useFormContext, useWatch } from "react-hook-form";

export const useAppSteps = (): WizardSteps<AppStepId> => {
    const connectSourceStep = useConnectSourceStep();
    const mapSchemaStep = useMapSchemaStep();

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

export const useAppFlow = (): AppStepId[] => {
    const { control } = useFormContext<AppFormData>();
    const dataSource = useWatch({
        control,
        name: "dataSource.source",
    });

    const mapSource = useWatch({
        control,
        name: "map.source",
    });

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
