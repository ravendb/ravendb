import { useFormContext } from "react-hook-form";
import { useParams } from "react-router";
import { usePrefetchQuery, useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { FormFieldsSkeleton } from "@/components/data/loading-skeletons";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { AddAiConnectionString } from "@/components/ai-connection-string/add-ai-connection-string";
import {
    getConnectionStringLabel,
    getServerConnectionStringName,
} from "@/components/ai-connection-string/ai-connection-string-utils";
import { FormCombobox } from "@/components/form/form-combobox";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";

export function ConnectProviderStep({ isBusy }: WizardBodyComponentProps) {
    const { slug = "" } = useParams();
    const { control } = useFormContext<AgentFormData>();

    usePrefetchQuery(api.queries.apps.suggestAgentFromData(slug));

    const connectionStringsQuery = useQuery(api.queries.apps.aiConnectionStringsList(slug));
    const items = connectionStringsQuery.data ?? [];

    return (
        <ApiState
            isLoading={connectionStringsQuery.isPending}
            isError={connectionStringsQuery.isError}
            errorTitle="Could not load connection strings"
            onRetry={connectionStringsQuery.refetch}
            loadingLabel="Loading connection strings..."
            skeleton={<FormFieldsSkeleton count={1} />}
        >
            {items.length === 0 ? (
                <Field>
                    <FieldLabel>Connection string</FieldLabel>
                    <div>
                        <AddButton />
                    </div>
                </Field>
            ) : (
                <div className="flex items-end gap-3">
                    <FormCombobox
                        control={control}
                        name="connection.connectionStringName"
                        label="Connection string"
                        className="flex-1"
                        placeholder="Select..."
                        disabled={isBusy}
                        options={items.map((item) => ({
                            value: item.name ?? "",
                            label: getConnectionStringLabel(item),
                        }))}
                        addons={<AddButton />}
                    />
                </div>
            )}
        </ApiState>
    );
}

function AddButton() {
    const { setValue } = useFormContext<AgentFormData>();

    return (
        <AddAiConnectionString
            modelType="Chat"
            onCreated={(name) =>
                setValue("connection.connectionStringName", getServerConnectionStringName(name), {
                    shouldValidate: true,
                    shouldDirty: true,
                })
            }
        />
    );
}
