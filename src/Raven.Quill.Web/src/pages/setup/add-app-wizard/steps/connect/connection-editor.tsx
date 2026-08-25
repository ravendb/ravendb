import { useFormContext, useWatch } from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import { FormToggleGroup, type FormToggleGroupOption } from "@/components/form/form-toggle-group";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { DEFAULT_PORT_BY_PROVIDER, type Provider, type SslMode } from "@/pages/setup/add-app-wizard/connection-string";
import { useConnectionSync } from "@/pages/setup/add-app-wizard/steps/connect/use-connection-sync";

const CONNECTION_STRING_PLACEHOLDER_BY_PROVIDER: Record<Provider, string> = {
    Npgsql: "Host=localhost;Port=5432;Database=my_db;Username=admin;Password=pass",
    SqlClient: "Server=localhost,1433;Database=my_db;User ID=sa;Password=pass",
    MySqlConnectorFactory: "Server=localhost;Port=3306;Database=my_db;User ID=admin;Password=pass",
};

const SSL_OPTIONS: FormToggleGroupOption<SslMode>[] = [
    { value: "default", label: "Driver default" },
    { value: "require", label: "Require" },
    { value: "disable", label: "Disable" },
];

const SSL_DEFAULT_DESCRIPTION_BY_PROVIDER: Record<Provider, string> = {
    Npgsql: "Driver default tries SSL first and falls back to an unencrypted connection.",
    SqlClient: "Driver default requires an encrypted connection.",
    MySqlConnectorFactory: "Driver default tries SSL first and falls back to an unencrypted connection.",
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
                    placeholder={provider ? String(DEFAULT_PORT_BY_PROVIDER[provider]) : undefined}
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
            <FormToggleGroup
                control={control}
                name="externalConnection.fields.ssl"
                label="SSL/TLS"
                options={SSL_OPTIONS}
                canDeselect={false}
                description={provider ? SSL_DEFAULT_DESCRIPTION_BY_PROVIDER[provider] : undefined}
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
            placeholder={provider ? CONNECTION_STRING_PLACEHOLDER_BY_PROVIDER[provider] : undefined}
            textareaClassName="font-mono text-xs"
            description="Switching to the connection details reads them from this string and keeps only the settings they can show."
            disabled={isDisabled}
        />
    );
}
