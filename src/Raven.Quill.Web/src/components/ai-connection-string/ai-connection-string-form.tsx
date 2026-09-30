import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm, useWatch } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { AiModelType } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { SheetClose, SheetFooter } from "@/components/shadcn/ui/sheet";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { withNestedSubmit } from "@/lib/form-utils";
import { invalidateAiConnectionStringQueries } from "@/lib/query-invalidation";
import {
    type ConnectionStringFormData,
    createConnectionStringSchema,
    getProviderOptions,
    mapFormDataToDto,
} from "@/components/ai-connection-string/ai-connection-string-utils";
import { TestAiConnectionStringButton } from "@/components/ai-connection-string/test-ai-connection-string-button";
import {
    ConnectionTestFailedError,
    useAiConnectionTest,
} from "@/components/ai-connection-string/use-ai-connection-test";
import { AzureOpenAiFields } from "@/components/ai-connection-string/provider-fields/azure-open-ai-fields";
import { EmbeddedFields } from "@/components/ai-connection-string/provider-fields/embedded-fields";
import { GoogleFields } from "@/components/ai-connection-string/provider-fields/google-fields";
import { HuggingFaceFields } from "@/components/ai-connection-string/provider-fields/hugging-face-fields";
import { MistralAiFields } from "@/components/ai-connection-string/provider-fields/mistral-ai-fields";
import { OllamaFields } from "@/components/ai-connection-string/provider-fields/ollama-fields";
import { OpenAiFields } from "@/components/ai-connection-string/provider-fields/open-ai-fields";
import { VertexFields } from "@/components/ai-connection-string/provider-fields/vertex-fields";

type AiConnectionStringFormProps = {
    modelType: AiModelType;
    defaultValues: ConnectionStringFormData;
    isEditing: boolean;
    // Preserved across an edit so re-saving (an upsert) keeps the stored id.
    existingIdentifier?: string;
    onSaved: (name: string) => void | Promise<void>;
};

function ProviderFields({
    provider,
    modelType,
}: {
    provider: ConnectionStringFormData["provider"];
    modelType: AiModelType;
}) {
    switch (provider) {
        case "openAiSettings":
            return <OpenAiFields modelType={modelType} />;
        case "azureOpenAiSettings":
            return <AzureOpenAiFields modelType={modelType} />;
        case "ollamaSettings":
            return <OllamaFields modelType={modelType} />;
        case "googleSettings":
            return <GoogleFields modelType={modelType} />;
        case "huggingFaceSettings":
            return <HuggingFaceFields />;
        case "mistralAiSettings":
            return <MistralAiFields />;
        case "vertexSettings":
            return <VertexFields />;
        case "embeddedSettings":
            return <EmbeddedFields />;
    }
}

export function AiConnectionStringForm({
    modelType,
    defaultValues,
    isEditing,
    existingIdentifier,
    onSaved,
}: AiConnectionStringFormProps) {
    const queryClient = useQueryClient();

    const form = useForm<ConnectionStringFormData>({
        mode: "onChange",
        resolver: zodResolver(createConnectionStringSchema(modelType)),
        defaultValues,
    });

    const provider = useWatch({ control: form.control, name: "provider" });

    const connectionTest = useAiConnectionTest(modelType, form);
    const unsavedChanges = useFormUnsavedChanges(form);

    const saveMutation = useMutation({
        mutationFn: async (values: ConnectionStringFormData) => {
            await connectionTest.ensureVerified(values);

            const dto = mapFormDataToDto(values, modelType);
            const result = await api.services.aiConnectionStrings.create(
                existingIdentifier ? { ...dto, identifier: existingIdentifier } : dto,
            );
            return result.name;
        },
        onSuccess: async (name) => {
            unsavedChanges.markSaved();
            await Promise.all([
                invalidateAiConnectionStringQueries(queryClient),
                // Refresh the cached detail so reopening the edit sheet shows the saved values.
                queryClient.invalidateQueries({
                    queryKey: api.queries.aiConnectionStrings.detail(name).queryKey,
                }),
            ]);
            await onSaved(name);
        },
    });

    return (
        <FormProvider {...form}>
            <form
                className="flex min-h-0 flex-1 flex-col"
                onSubmit={withNestedSubmit(form.handleSubmit((values) => saveMutation.mutateAsync(values)))}
            >
                <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                    <FormInput
                        control={form.control}
                        name="name"
                        label="Name"
                        placeholder="e.g. OpenAI Production"
                        disabled={isEditing}
                        description={
                            isEditing ? "The name identifies the connection string and can't be changed." : undefined
                        }
                    />
                    <FormSelect
                        control={form.control}
                        name="provider"
                        label="Provider"
                        options={getProviderOptions(modelType)}
                    />
                    <ProviderFields provider={provider} modelType={modelType} />

                    <TestAiConnectionStringButton
                        isVerified={connectionTest.isVerified}
                        isPending={connectionTest.isPending}
                        error={connectionTest.error}
                        disabled={saveMutation.isPending}
                        onTest={connectionTest.test}
                    />

                    {saveMutation.error && !(saveMutation.error instanceof ConnectionTestFailedError) && (
                        <Alert variant="destructive">
                            {saveMutation.error instanceof Error
                                ? saveMutation.error.message
                                : "Could not save connection string."}
                        </Alert>
                    )}
                </div>

                <SheetFooter className="flex-row justify-end border-t">
                    <SheetClose asChild>
                        <Button type="button" variant="outline">
                            Cancel
                        </Button>
                    </SheetClose>
                    <Button type="submit" disabled={saveMutation.isPending}>
                        {saveMutation.isPending && <Spinner />}
                        {isEditing ? "Save changes" : "Save"}
                    </Button>
                </SheetFooter>
            </form>
        </FormProvider>
    );
}
