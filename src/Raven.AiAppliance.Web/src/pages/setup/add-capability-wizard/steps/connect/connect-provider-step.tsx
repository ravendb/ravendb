import { useEffect, useRef } from "react";
import { useFormContext } from "react-hook-form";
import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { StepSection } from "@/pages/setup/add-app-wizard/app-wizard-step-section";
import { AddConnectionStringDialog } from "@/pages/setup/add-capability-wizard/steps/connect/add-connection-string-dialog";
import { FormCombobox } from "@/components/form/form-combobox";

export function ConnectProviderStep(props: WizardBodyComponentProps) {
    const { slug = "" } = useParams();
    const { control, setValue } = useFormContext<AgentFormData>();
    const createdConnectionStringNameRef = useRef<string>(undefined);

    const connectionStringsQuery = useQuery(api.queries.aiConnectionStrings.list(slug));
    const items = connectionStringsQuery.data?.items ?? [];
    const isPending = props.status === "pending";

    // TODO fixme
    // Workaround for now to select the newly added connection string
    useEffect(() => {
        const createdConnectionStringName = createdConnectionStringNameRef.current;

        if (
            !createdConnectionStringName ||
            !connectionStringsQuery.data?.items.some((item) => item.name === createdConnectionStringName)
        ) {
            return;
        }

        setValue("connection.connectionStringName", createdConnectionStringName, {
            shouldValidate: true,
            shouldDirty: true,
        });
        createdConnectionStringNameRef.current = undefined;
    }, [connectionStringsQuery.data?.items, setValue]);

    return (
        <StepSection {...props}>
            <ApiState
                isLoading={connectionStringsQuery.isPending}
                isError={connectionStringsQuery.isError}
                errorTitle="Could not load connection strings"
                onRetry={connectionStringsQuery.refetch}
                loadingLabel="Loading connection strings..."
            >
                <div className="flex items-end gap-3">
                    <FormCombobox
                        control={control}
                        name="connection.connectionStringName"
                        label="Connection string"
                        className="flex-1"
                        placeholder={items.length > 0 ? "Select..." : "No connection strings yet"}
                        disabled={isPending || items.length === 0}
                        options={items.map((item) => ({
                            value: item.name,
                            label: `${item.name} · ${item.provider}`,
                        }))}
                        addons={
                            <AddConnectionStringDialog
                                slug={slug}
                                onCreated={async (name) => {
                                    createdConnectionStringNameRef.current = name;
                                    await connectionStringsQuery.refetch();
                                }}
                            />
                        }
                    />
                </div>
            </ApiState>
        </StepSection>
    );
}
