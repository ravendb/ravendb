import { useFormContext } from "react-hook-form";
import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { AddAiConnectionString } from "@/components/ai-connection-string/add-ai-connection-string";
import { FormCombobox } from "@/components/form/form-combobox";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";

export function ConnectProviderStep({ isBusy }: WizardBodyComponentProps) {
    const { slug = "" } = useParams();
    const { control } = useFormContext<AgentFormData>();
    const connectionStringsQuery = useQuery(api.queries.aiConnectionStrings.list(slug));
    const items = connectionStringsQuery.data?.items ?? [];

    return (
        <ApiState
            isLoading={connectionStringsQuery.isPending}
            isError={connectionStringsQuery.isError}
            errorTitle="Could not load connection strings"
            onRetry={connectionStringsQuery.refetch}
            loadingLabel="Loading connection strings..."
        >
            {items.length === 0 ? (
                <Field>
                    <FieldLabel>Connection string</FieldLabel>
                    <div>
                        <AddButton slug={slug} />
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
                            value: item.name,
                            label: `${item.name} · ${item.provider}`,
                        }))}
                        addons={<AddButton slug={slug} />}
                    />
                </div>
            )}
        </ApiState>
    );
}

function AddButton({ slug }: { slug: string }) {
    const { setValue } = useFormContext<AgentFormData>();

    return (
        <AddAiConnectionString
            slug={slug}
            modelType="Chat"
            onCreated={(name) =>
                setValue("connection.connectionStringName", name, {
                    shouldValidate: true,
                    shouldDirty: true,
                })
            }
        />
    );
}
