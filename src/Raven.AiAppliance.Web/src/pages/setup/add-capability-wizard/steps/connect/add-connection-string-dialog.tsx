import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useWatch } from "react-hook-form";
import { z } from "zod";
import { useMutation } from "@tanstack/react-query";
import { Plus } from "lucide-react";
import { api } from "@/api/api";
import type { AiConnectionString } from "@/api/generated/server-api";
import { Button } from "@/components/shadcn/ui/button";
import { Alert } from "@/components/shadcn/ui/alert";
import { Spinner } from "@/components/shadcn/ui/spinner";
import {
    Dialog,
    DialogClose,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/shadcn/ui/dialog";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { withNestedSubmit } from "@/lib/form-utils";

// The provision endpoint only accepts OpenAi/Ollama Chat connection strings in the current
// demo, so the dialog offers exactly those two providers.
const connectionStringSchema = z
    .object({
        name: z.string().trim().min(1, "Name is required"),
        provider: z.union([z.literal("openai"), z.literal("ollama")]),
        model: z.string().trim().min(1, "Model is required"),
        apiKey: z.string(),
        uri: z.string(),
    })
    .superRefine((values, ctx) => {
        if (values.provider === "openai" && values.apiKey.trim().length === 0) {
            ctx.addIssue({ code: "custom", path: ["apiKey"], message: "API key is required" });
        }
        if (values.provider === "ollama" && values.uri.trim().length === 0) {
            ctx.addIssue({ code: "custom", path: ["uri"], message: "Ollama URI is required" });
        }
    });

type ConnectionStringFormData = z.infer<typeof connectionStringSchema>;

const PROVIDER_OPTIONS: FormSelectOption<ConnectionStringFormData["provider"]>[] = [
    { value: "openai", label: "OpenAI" },
    { value: "ollama", label: "Ollama" },
];

type AddConnectionStringDialogProps = {
    slug: string;
    onCreated: (name: string) => Promise<void>;
};

export function AddConnectionStringDialog({ slug, onCreated }: AddConnectionStringDialogProps) {
    const [isOpen, setIsOpen] = useState(false);

    const form = useForm<ConnectionStringFormData>({
        mode: "onChange",
        resolver: zodResolver(connectionStringSchema),
        defaultValues: {
            name: "",
            provider: "openai",
            model: "gpt-4o-mini",
            apiKey: "",
            uri: "http://localhost:11434/",
        },
    });

    const provider = useWatch({ control: form.control, name: "provider" });

    const createMutation = useMutation({
        mutationFn: async (values: ConnectionStringFormData) => {
            const body: AiConnectionString = {
                name: values.name.trim(),
                modelType: "Chat",
                ...(values.provider === "openai"
                    ? { openAiSettings: { apiKey: values.apiKey.trim(), model: values.model.trim() } }
                    : { ollamaSettings: { uri: values.uri.trim(), model: values.model.trim() } }),
            };

            const result = await api.services.aiConnectionStrings.create(slug, body);
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
        <Dialog
            open={isOpen}
            onOpenChange={(open) => {
                setIsOpen(open);
                if (!open) {
                    form.reset();
                    createMutation.reset();
                }
            }}
        >
            <DialogTrigger asChild>
                <Button type="button" variant="secondary">
                    <Plus className="size-3.5" aria-hidden="true" />
                    Add connection string
                </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-md">
                <DialogHeader>
                    <DialogTitle>Add AI connection string</DialogTitle>
                    <DialogDescription>
                        Connect a Chat model. Only OpenAI and Ollama are supported in this preview.
                    </DialogDescription>
                </DialogHeader>

                <form
                    className="grid gap-4"
                    onSubmit={withNestedSubmit(form.handleSubmit((values) => createMutation.mutateAsync(values)))}
                >
                    <FormInput control={form.control} name="name" label="Name" placeholder="e.g. ChatGPT" />
                    <FormSelect control={form.control} name="provider" label="Provider" options={PROVIDER_OPTIONS} />
                    <FormInput control={form.control} name="model" label="Model" placeholder="e.g. gpt-4o-mini" />
                    {provider === "openai" ? (
                        <FormInput
                            control={form.control}
                            name="apiKey"
                            label="API key"
                            type="password"
                            placeholder="sk-..."
                        />
                    ) : (
                        <FormInput
                            control={form.control}
                            name="uri"
                            label="Ollama URI"
                            placeholder="http://localhost:11434/"
                        />
                    )}

                    {createMutation.isError && (
                        <Alert variant="destructive">
                            {createMutation.error instanceof Error
                                ? createMutation.error.message
                                : "Could not create connection string."}
                        </Alert>
                    )}

                    <DialogFooter>
                        <DialogClose asChild>
                            <Button type="button" variant="outline">
                                Cancel
                            </Button>
                        </DialogClose>
                        <Button type="submit" disabled={createMutation.isPending}>
                            {createMutation.isPending && <Spinner />}
                            Add
                        </Button>
                    </DialogFooter>
                </form>
            </DialogContent>
        </Dialog>
    );
}
