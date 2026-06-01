import type { WizardSteps } from "@/components/form/wizard/form-wizard";
import { ChooseDataSourceStep } from "@/pages/setup/add-app-wizard/steps/choose-data-source-step";
import { ConnectSourceStep, useConnectSourceStep } from "@/pages/setup/add-app-wizard/steps/connect-source-step";
import { PreviewStep } from "@/pages/setup/add-app-wizard/steps/preview-step";
import { VerifySchemaStep } from "@/pages/setup/add-app-wizard/steps/verify-schema-step";
import { useFormContext, useWatch } from "react-hook-form";
import { z } from "zod";

export const appSchema = z.object({
    dataSource: z.object({
        source: z.union([z.literal("external"), z.literal("ravendb")]),
    }),
    externalConnection: z.object({
        appName: z.string().trim().min(1, "Application name is required"),
        provider: z.union([z.literal("Npgsql"), z.literal("SqlClient"), z.literal("MySqlConnectorFactory")]),
        connectionString: z.string().trim().min(1, "Connection string is required."),
    }),
    verifySchema: z.object({
        tables: z.array(
            z.object({
                sourceTableSchema: z.string().nullable().optional(),
                sourceTableName: z.string(),
            }),
        ),
    }),
    howToMap: z.object({
        source: z.union([z.literal("ai-suggested"), z.literal("manual")]),
        aiPrompt: z.string(),
    }),
    map: z.object({
        tables: z.array(z.any()),
    }),
    preview: z.object({
        table: z.string(),
    }),
});

export type AppFormData = z.infer<typeof appSchema>;
export type AppStepId = keyof AppFormData;

export const useAppSteps = (): WizardSteps<AppStepId> => {
    const connectSourceStep = useConnectSourceStep();

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
        howToMap: {
            id: "howToMap",
            title: "How would you like to map your schema?",
            bodyComponent: () => <div>howToMap</div>,
        },
        map: {
            id: "map",
            title: "Map schema",
            bodyComponent: () => <div>Map schema</div>,
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
    const source = useWatch({
        control,
        name: "dataSource.source",
    });

    if (source === "ravendb") {
        return ["dataSource", "preview"];
    }

    return ["dataSource", "externalConnection", "verifySchema", "howToMap", "map", "preview"];
};
