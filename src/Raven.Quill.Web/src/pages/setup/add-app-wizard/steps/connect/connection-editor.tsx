import { useFormContext, useWatch } from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { FormSwitch } from "@/components/form/form-switch";
import { FormTextarea } from "@/components/form/form-textarea";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { DEFAULT_PORT_BY_PROVIDER } from "@/pages/setup/add-app-wizard/connection-string";
import { useConnectionSync } from "@/pages/setup/add-app-wizard/steps/connect/use-connection-sync";

type Provider = AppFormData["externalConnection"]["provider"];

const CONNECTION_STRING_PLACEHOLDER_BY_PROVIDER: Record<Provider, string> = {
    Npgsql: "Host=localhost;Port=5432;Database=my_db;Username=admin;Password=pass",
    SqlClient: "Server=localhost,1433;Database=my_db;User ID=sa;Password=pass",
    MySqlConnectorFactory: "Server=localhost;Port=3306;Database=my_db;User ID=admin;Password=pass",
};

export function ConnectionEditor({ isDisabled }: { isDisabled: boolean }) {
    const { control } = useFormContext<AppFormData>();
    const mode = useWatch({ control, name: "externalConnection.mode" });
    const { changeMode } = useConnectionSync();

    return (
        <Tabs value={mode} onValueChange={(next) => changeMode(next as AppFormData["externalConnection"]["mode"])}>
            <TabsList>
                <TabsTrigger value="fields" disabled={isDisabled}>
                    Connection details
                </TabsTrigger>
                <TabsTrigger value="raw" disabled={isDisabled}>
                    Connection string
                </TabsTrigger>
            </TabsList>
            <TabsContent value="fields" className="grid gap-5 pt-2">
                <ConnectionFieldsEditor isDisabled={isDisabled} />
            </TabsContent>
            <TabsContent value="raw" className="grid gap-5 pt-2">
                <ConnectionStringEditor isDisabled={isDisabled} />
            </TabsContent>
        </Tabs>
    );
}

function ConnectionFieldsEditor({ isDisabled }: { isDisabled: boolean }) {
    const { control } = useFormContext<AppFormData>();
    const provider = useWatch({ control, name: "externalConnection.provider" });

    return (
        <>
            <div className="grid gap-5 sm:grid-cols-[minmax(0,1fr)_9rem]">
                <FormInput
                    control={control}
                    name="externalConnection.fields.host"
                    label="Host"
                    placeholder="e.g. db.example.com"
                    disabled={isDisabled}
                />
                <FormInput
                    control={control}
                    name="externalConnection.fields.port"
                    label="Port"
                    type="number"
                    placeholder={String(DEFAULT_PORT_BY_PROVIDER[provider])}
                    disabled={isDisabled}
                />
            </div>
            <FormInput
                control={control}
                name="externalConnection.fields.database"
                label="Database"
                placeholder="e.g. acme_shop"
                disabled={isDisabled}
            />
            <div className="grid gap-5 sm:grid-cols-2">
                <FormInput
                    control={control}
                    name="externalConnection.fields.username"
                    label="Username"
                    placeholder="e.g. admin"
                    disabled={isDisabled}
                />
                <FormInput
                    control={control}
                    name="externalConnection.fields.password"
                    label="Password"
                    type="password"
                    disabled={isDisabled}
                />
            </div>
            <FormSwitch
                control={control}
                name="externalConnection.fields.isSecured"
                label="Secured connection (SSL/TLS)"
                disabled={isDisabled}
            />
        </>
    );
}

function ConnectionStringEditor({ isDisabled }: { isDisabled: boolean }) {
    const { control } = useFormContext<AppFormData>();
    const provider = useWatch({ control, name: "externalConnection.provider" });

    return (
        <FormTextarea
            control={control}
            name="externalConnection.connectionString"
            label="Connection string"
            labelClassName="sr-only"
            placeholder={CONNECTION_STRING_PLACEHOLDER_BY_PROVIDER[provider]}
            textareaClassName="font-mono text-xs"
            description="Switching to the connection details reads them from this string and keeps only the settings they can show."
            disabled={isDisabled}
        />
    );
}
