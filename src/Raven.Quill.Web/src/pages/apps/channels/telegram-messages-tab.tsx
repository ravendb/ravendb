import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import { SectionEditActions } from "@/pages/apps/channels/channel-tab-actions";
import { EditableTabShell } from "@/pages/apps/channels/editable-tab-shell";
import {
    TELEGRAM_MESSAGE_GROUPS,
    telegramMessagesSchema,
    toMessagesDto,
    toMessagesFormValues,
} from "@/pages/apps/channels/telegram-message-defaults";
import { Heading, Text } from "@/components/typography";

const messagesFormSchema = z.object({ messages: telegramMessagesSchema });
type MessagesFormData = z.infer<typeof messagesFormSchema>;

// The Telegram bot's canned replies, shown read-only until "Edit" is pressed. This used to live in a
// collapsible inside the edit sheet; it moved here so the sheet keeps only the channel basics,
// mirroring how the web widget's appearance lives in its own tab.
export function TelegramMessagesTab({ slug, channel }: { slug: string; channel: ChannelSummaryResponse }) {
    const queryClient = useQueryClient();
    const [isEditing, setIsEditing] = useState(false);

    const form = useForm<MessagesFormData>({
        mode: "onChange",
        resolver: zodResolver(messagesFormSchema),
        defaultValues: { messages: toMessagesFormValues(channel.telegram?.messages) },
    });

    const unsavedChanges = useFormUnsavedChanges(form);

    const updateMutation = useMutation({
        // Partial update: only the Telegram messages change, everything else is left untouched.
        mutationFn: (values: MessagesFormData) =>
            api.services.channels.update(slug, channel.channelId, {
                displayName: null,
                allowedOrigins: null,
                enabled: null,
                telegram: { botToken: null, messages: toMessagesDto(values.messages) },
            }),
        onSuccess: async () => {
            unsavedChanges.markSaved();
            await invalidateChannelQueries(queryClient, slug, channel.type);
            toast.success("Bot messages saved");
            setIsEditing(false);
        },
    });

    const onCancel = () => {
        form.reset();
        setIsEditing(false);
    };

    return (
        <form
            className="flex min-h-0 flex-1 flex-col"
            onSubmit={form.handleSubmit((values) => updateMutation.mutate(values))}
        >
            <EditableTabShell
                title="Message templates"
                description="Customize what the bot says. Leave a field empty to use the default text."
                actions={
                    <SectionEditActions
                        isEditing={isEditing}
                        isSaving={updateMutation.isPending}
                        onEdit={() => setIsEditing(true)}
                        onCancel={onCancel}
                    />
                }
            >
                <div className="flex flex-col gap-4">
                    {TELEGRAM_MESSAGE_GROUPS.map((group) => (
                        <section
                            key={group.title}
                            className="grid gap-x-16 gap-y-4 rounded-md border bg-card p-4 md:grid-cols-[minmax(0,22rem)_1fr] md:p-5"
                        >
                            <div className="space-y-0.5">
                                <Heading as="h3" variant="subsection">
                                    {group.title}
                                </Heading>
                                <Text variant="muted">{group.description}</Text>
                            </div>
                            <div className="grid gap-4">
                                {group.fields.map((field) =>
                                    field.kind === "line" ? (
                                        <FormInput
                                            key={field.name}
                                            control={form.control}
                                            name={`messages.${field.name}`}
                                            label={field.label}
                                            placeholder={field.defaultText}
                                            disabled={!isEditing}
                                        />
                                    ) : (
                                        <FormTextarea
                                            key={field.name}
                                            control={form.control}
                                            name={`messages.${field.name}`}
                                            label={field.label}
                                            placeholder={field.defaultText}
                                            rows={2}
                                            disabled={!isEditing}
                                        />
                                    ),
                                )}
                            </div>
                        </section>
                    ))}
                </div>

                {updateMutation.isError && (
                    <Alert variant="destructive" className="mt-3">
                        {updateMutation.error instanceof Error
                            ? updateMutation.error.message
                            : "Could not save the bot messages."}
                    </Alert>
                )}
            </EditableTabShell>
        </form>
    );
}
