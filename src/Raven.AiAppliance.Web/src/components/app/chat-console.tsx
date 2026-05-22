import { zodResolver } from "@hookform/resolvers/zod";
import { Plus, Send, Trash2 } from "lucide-react";
import { useState } from "react";
import { useFieldArray, useForm } from "react-hook-form";
import { toast } from "sonner";
import { z } from "zod";
import { api } from "@/api/api";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";

type ChatConsoleProps = {
    defaultAgentId?: string;
};

export function ChatConsole({ defaultAgentId = "" }: ChatConsoleProps) {
    const [answer, setAnswer] = useState("");
    const [isStreaming, setIsStreaming] = useState(false);
    const { control, handleSubmit, resetField, setValue } = useForm<ChatConsoleFormValues>({
        defaultValues: {
            agentId: defaultAgentId,
            conversationId: "",
            parameters: [],
            prompt: "",
        },
        resolver: zodResolver(chatConsoleSchema),
    });
    const parameterFields = useFieldArray({
        control,
        name: "parameters",
    });

    async function sendPrompt(values: ChatConsoleFormValues) {
        setIsStreaming(true);
        setAnswer("");

        try {
            let streamedText = "";
            for await (const streamEvent of api.services.chat.stream({
                agentId: values.agentId.trim(),
                conversationId: values.conversationId.trim() || null,
                parameters: parseParameters(values.parameters),
                prompt: values.prompt.trim(),
            })) {
                if (streamEvent.type === "chunk") {
                    streamedText += streamEvent.text;
                    setAnswer((value) => `${value}${streamEvent.text}`);
                }

                if (streamEvent.type === "done") {
                    setValue("conversationId", streamEvent.conversationId);
                    if (!streamedText && streamEvent.answer) {
                        setAnswer(JSON.stringify(streamEvent.answer, null, 2));
                    }
                }

                if (streamEvent.type === "error") {
                    toast.error(streamEvent.message);
                }
            }
        } catch (error) {
            toast.error(error instanceof Error ? error.message : "Chat request failed.");
        } finally {
            setIsStreaming(false);
        }
    }

    return (
        <form className="grid gap-4" onSubmit={handleSubmit(sendPrompt)}>
            <div className="grid gap-3 md:grid-cols-2">
                <FormInput control={control} name="agentId" label="Agent id" />
                <FormInput control={control} name="conversationId" label="Conversation id" />
            </div>

            <Field>
                <div className="flex items-center justify-between gap-3">
                    <FieldLabel>Parameters</FieldLabel>
                    <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() =>
                            parameterFields.append({
                                key: "",
                                value: "",
                            })
                        }
                    >
                        <Plus className="size-4" aria-hidden />
                        Add
                    </Button>
                </div>
                {parameterFields.fields.length === 0 ? (
                    <div className="rounded-md border bg-background px-3 py-4 text-center text-sm text-muted-foreground">
                        No parameters.
                    </div>
                ) : (
                    <div className="grid gap-2">
                        {parameterFields.fields.map((field, index) => (
                            <div key={field.id} className="grid gap-2 md:grid-cols-[1fr_1fr_auto]">
                                <FormInput
                                    control={control}
                                    name={`parameters.${index}.key`}
                                    label={index === 0 ? "Key" : undefined}
                                />
                                <FormInput
                                    control={control}
                                    name={`parameters.${index}.value`}
                                    label={index === 0 ? "Value" : undefined}
                                />
                                <Button
                                    type="button"
                                    variant="ghost"
                                    size="icon"
                                    className="self-end text-destructive"
                                    onClick={() => parameterFields.remove(index)}
                                    aria-label="Remove parameter"
                                    title="Remove parameter"
                                >
                                    <Trash2 className="size-4" aria-hidden />
                                </Button>
                            </div>
                        ))}
                    </div>
                )}
            </Field>

            <FormTextarea control={control} name="prompt" label="Prompt" className="min-h-28" />

            <div className="flex justify-end gap-2">
                <Button
                    type="button"
                    variant="outline"
                    onClick={() => {
                        setAnswer("");
                        resetField("prompt");
                    }}
                >
                    <Trash2 className="size-4" aria-hidden="true" />
                    Clear
                </Button>
                <Button disabled={isStreaming}>
                    <Send className="size-4" aria-hidden="true" />
                    {isStreaming ? "Sending..." : "Send"}
                </Button>
            </div>

            <section className="min-h-40 rounded-md border bg-background p-4">
                <pre className="text-sm whitespace-pre-wrap">{answer || "No answer yet."}</pre>
            </section>
        </form>
    );
}

function parseParameters(parameters: ChatConsoleFormValues["parameters"]) {
    const entries = parameters
        .map((parameter) => [parameter.key.trim(), parameter.value] as const)
        .filter(([key]) => key);

    return entries.length ? Object.fromEntries(entries) : null;
}

const chatConsoleSchema = z.object({
    agentId: z.string().trim().min(1, "Agent id is required."),
    conversationId: z.string(),
    parameters: z.array(
        z.object({
            key: z.string(),
            value: z.string(),
        }),
    ),
    prompt: z.string().trim().min(1, "Prompt is required."),
});

type ChatConsoleFormValues = z.infer<typeof chatConsoleSchema>;
