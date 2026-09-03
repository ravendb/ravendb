import { AppWizard } from "@/pages/setup/add-app-wizard/app-wizard";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

export function AddAppWizard() {
    return <AppWizard defaultValues={getDefaultValues()} />;
}

function getDefaultValues(): AppFormData {
    return {
        dataSource: {
            source: "external",
        },
        externalConnection: {
            appName: "",
            slug: "",
            provider: "",
            mode: "fields",
            fields: {
                host: "",
                port: null,
                database: "",
                username: "",
                password: "",
                ssl: "default",
            },
            connectionString: "",
        },
        verifySchema: {
            tables: [],
        },
        map: {
            source: "ai-suggested",
            aiPrompt: "",
        },
        mapTables: {
            tables: [],
        },
        preview: {
            table: "",
            maxRows: 1,
        },
    };
}
