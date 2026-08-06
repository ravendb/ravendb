import { AppWizard } from "@/pages/setup/add-app-wizard/app-wizard";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { DEFAULT_PORT_BY_PROVIDER, DEFAULT_PROVIDER } from "@/pages/setup/add-app-wizard/connection-string";

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
            provider: DEFAULT_PROVIDER,
            mode: "fields",
            fields: {
                host: "",
                port: DEFAULT_PORT_BY_PROVIDER[DEFAULT_PROVIDER],
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
