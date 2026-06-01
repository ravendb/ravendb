/* eslint-disable react-refresh/only-export-components */
import { useMutation } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";
import { api } from "@/api/api";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormTextarea } from "@/components/form/form-textarea";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";

export function ConnectSourceStep(props: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();

    return (
        <StepSection {...props}>
            <div className="grid gap-5">
                <FormInput
                    control={control}
                    name="externalConnection.appName"
                    label="Application name"
                    placeholder="e.g. AcmeShop"
                    disabled={props.status === "pending"}
                />
                <FormSelect
                    control={control}
                    name="externalConnection.provider"
                    label="Database type"
                    options={PROVIDER_OPTIONS}
                    disabled={props.status === "pending"}
                />
                <FormTextarea
                    control={control}
                    name="externalConnection.connectionString"
                    label="Connection string"
                    placeholder="Host=localhost;Port=5432;Database=my_db;Username=admin;Password=pass"
                    textareaClassName="font-mono text-xs"
                    disabled={props.status === "pending"}
                />
            </div>
        </StepSection>
    );
}

const PROVIDER_OPTIONS: FormSelectOption<AppFormData["externalConnection"]["provider"]>[] = [
    {
        value: "Npgsql",
        label: "PostgreSQL",
    },
    {
        value: "SqlClient",
        label: "SQL Server",
    },
    {
        value: "MySqlConnectorFactory",
        label: "MySQL",
    },
];

export function useConnectSourceStep() {
    const { getValues } = useFormContext<AppFormData>();
    const setDiscoverResult = useSetupWizardStore((state) => state.setDiscoverResult);

    const connectAndDiscover = useMutation({
        mutationFn: async () => {
            const formValues = getValues("externalConnection");

            const connectResult = await api.services.setup.connect({
                connectionString: formValues.connectionString,
                provider: formValues.provider,
                tableNames: ["users", "orders"], // TODO null
            });

            if (!connectResult.success) {
                throw Error(connectResult.errors?.join("\n") || "Connection failed.");
            }

            const discoverResult = await api.services.setup.discover({
                connectionString: formValues.connectionString,
                provider: formValues.provider,
                tableNames: ["users", "orders"], // TODO null
            });

            setDiscoverResult(discoverResult);

            return true;
        },
    });

    return connectAndDiscover;
}
