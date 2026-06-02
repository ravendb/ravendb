import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm, useWatch } from "react-hook-form";
import { useMutation } from "@tanstack/react-query";
import { Plus } from "lucide-react";
import { api } from "@/api/api";
import type { AiModelType } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import {
    Sheet,
    SheetClose,
    SheetContent,
    SheetDescription,
    SheetFooter,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/shadcn/ui/sheet";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { withNestedSubmit } from "@/lib/form-utils";
import {
    type ConnectionStringFormData,
    createConnectionStringSchema,
    getDefaultValues,
    getProviderOptions,
    mapFormDataToDto,
} from "@/pages/setup/add-capability-wizard/steps/connect/ai-connection-string-utils";
import { AzureOpenAiFields } from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/azure-open-ai-fields";
import { EmbeddedFields } from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/embedded-fields";
import { GoogleFields } from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/google-fields";
import { HuggingFaceFields } from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/hugging-face-fields";
import { MistralAiFields } from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/mistral-ai-fields";
import { OllamaFields } from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/ollama-fields";
import { OpenAiFields } from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/open-ai-fields";
import { VertexFields } from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/vertex-fields";

type AddAiConnectionStringProps = {
    slug: string;
    modelType: AiModelType;
    onCreated: (name: string) => Promise<void>;
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

export function AddAiConnectionString({ slug, modelType, onCreated }: AddAiConnectionStringProps) {
    const [isOpen, setIsOpen] = useState(false);

    const form = useForm<ConnectionStringFormData>({
        mode: "onChange",
        resolver: zodResolver(createConnectionStringSchema(modelType)),
        defaultValues: getDefaultValues(),
    });

    const provider = useWatch({ control: form.control, name: "provider" });

    const createMutation = useMutation({
        mutationFn: async (values: ConnectionStringFormData) => {
            const result = await api.services.aiConnectionStrings.create(slug, mapFormDataToDto(values, modelType));
            return result.name;
        },
        onSuccess: async (name) => {
            await onCreated(name);
            form.reset();
            createMutation.reset();
            setIsOpen(false);
        },
    });

    return (
        <Sheet
            open={isOpen}
            onOpenChange={(open) => {
                setIsOpen(open);
                if (!open) {
                    form.reset();
                    createMutation.reset();
                }
            }}
        >
            <SheetTrigger asChild>
                <Button type="button" variant="secondary">
                    <Plus className="size-3.5" aria-hidden="true" />
                    Add connection string
                </Button>
            </SheetTrigger>
            <SheetContent className="w-full gap-0 sm:max-w-lg data-[side=right]:sm:max-w-lg">
                <SheetHeader className="border-b">
                    <SheetTitle>Add connection string</SheetTitle>
                    <SheetDescription>Pick a provider and fill in the connection details.</SheetDescription>
                </SheetHeader>

                <FormProvider {...form}>
                    <form
                        className="flex min-h-0 flex-1 flex-col"
                        onSubmit={withNestedSubmit(form.handleSubmit((values) => createMutation.mutateAsync(values)))}
                    >
                        <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                            <FormInput
                                control={form.control}
                                name="name"
                                label="Name"
                                placeholder="e.g. OpenAI Production"
                            />
                            <FormSelect
                                control={form.control}
                                name="provider"
                                label="Provider"
                                options={getProviderOptions(modelType)}
                            />
                            <ProviderFields provider={provider} modelType={modelType} />

                            {createMutation.isError && (
                                <Alert variant="destructive">
                                    {createMutation.error instanceof Error
                                        ? createMutation.error.message
                                        : "Could not create connection string."}
                                </Alert>
                            )}
                        </div>

                        <SheetFooter className="flex-row justify-end border-t">
                            <SheetClose asChild>
                                <Button type="button" variant="outline">
                                    Cancel
                                </Button>
                            </SheetClose>
                            <Button type="submit" disabled={createMutation.isPending}>
                                {createMutation.isPending && <Spinner />}
                                Save
                            </Button>
                        </SheetFooter>
                    </form>
                </FormProvider>
            </SheetContent>
        </Sheet>
    );
}
