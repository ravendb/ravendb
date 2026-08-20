import type { ReactNode } from "react";
import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { ChevronDown } from "lucide-react";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { FormInput } from "@/components/form/form-input";
import { FormStringList } from "@/components/form/form-string-list";
import { FormTextarea } from "@/components/form/form-textarea";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { withNestedSubmit } from "@/lib/form-utils";
import { cn } from "@/lib/utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import {
    TELEGRAM_MESSAGE_FIELDS,
    telegramMessagesSchema,
    toMessagesDto,
    toMessagesFormValues,
} from "@/pages/apps/channels/telegram-message-defaults";

const editChannelSchema = z.object({
    displayName: z.string().trim().min(1, "Channel name is required"),
    allowedOrigins: z.array(z.object({ value: z.string().trim() })),
    botToken: z.string().trim(),
    messages: telegramMessagesSchema,
});

type EditChannelFormData = z.infer<typeof editChannelSchema>;

// The channel's editable settings, rendered inside the edit sheet. The caller owns the surrounding
// chrome by supplying container classes and the footer (which receives the pending state so it can
// disable/spin its submit button). The visible fields depend on the channel type: web widgets edit
// their allowed origins, Telegram bots rotate their token and customize their canned messages.
export function ChannelConfigForm({
    slug,
    channel,
    onSaved,
    className,
    bodyClassName,
    footer,
}: {
    slug: string;
    channel: ChannelSummaryResponse;
    onSaved?: () => void;
    className?: string;
    bodyClassName?: string;
    footer: (state: { isPending: boolean }) => ReactNode;
}) {
    const queryClient = useQueryClient();

    const isTelegram = channel.type === "Telegram";
    const [areMessagesOpen, setAreMessagesOpen] = useState(false);

    const form = useForm<EditChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(editChannelSchema),
        defaultValues: {
            displayName: channel.displayName,
            allowedOrigins: (channel.allowedOrigins ?? []).map((value) => ({ value })),
            botToken: "",
            messages: toMessagesFormValues(channel.telegram?.messages),
        },
    });

    const unsavedChanges = useFormUnsavedChanges(form);

    const updateMutation = useMutation({
        mutationFn: (values: EditChannelFormData) =>
            // Update is a partial edit: null fields are left unchanged on the server.
            api.services.channels.update(slug, channel.channelId, {
                displayName: values.displayName.trim(),
                allowedOrigins: isTelegram
                    ? null
                    : values.allowedOrigins.map((origin) => origin.value.trim()).filter(Boolean),
                // Enabled state is owned by the header's Pause/Resume toggle; null leaves it untouched.
                enabled: null,
                telegram: isTelegram
                    ? {
                          botToken: values.botToken.trim() || null,
                          messages: toMessagesDto(values.messages),
                      }
                    : null,
            }),
        onSuccess: async () => {
            unsavedChanges.markSaved();
            await invalidateChannelQueries(queryClient, slug);
            toast.success("Channel updated");
            onSaved?.();
        },
    });

    return (
        <form
            className={className}
            onSubmit={withNestedSubmit(form.handleSubmit((values) => updateMutation.mutate(values)))}
        >
            <div className={cn("flex flex-col gap-4", bodyClassName)}>
                <FormInput
                    control={form.control}
                    name="displayName"
                    label="Channel name"
                    placeholder="e.g. Storefront help"
                    description="Shown in the channels list."
                />
                {isTelegram ? (
                    <>
                        <FormInput
                            control={form.control}
                            name="botToken"
                            type="password"
                            label="Rotate bot token"
                            placeholder="Leave empty to keep the current token"
                            description="Paste a new token from @BotFather to rotate it. The current token is never shown."
                        />
                        <Collapsible open={areMessagesOpen} onOpenChange={setAreMessagesOpen} className="grid gap-3">
                            <CollapsibleTrigger className="group flex w-full items-start justify-between gap-3 text-left">
                                <div>
                                    <h3 className="text-sm font-semibold">Bot messages</h3>
                                    <p className="mt-1 text-xs text-muted-foreground">
                                        The canned replies this bot sends for commands and parameter prompts. Leave a
                                        field empty to use the default text.
                                    </p>
                                </div>
                                <ChevronDown
                                    className="mt-0.5 size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                                    aria-hidden="true"
                                />
                            </CollapsibleTrigger>
                            <CollapsibleContent className="grid gap-3">
                                {TELEGRAM_MESSAGE_FIELDS.map((field) => (
                                    <FormTextarea
                                        key={field.name}
                                        control={form.control}
                                        name={`messages.${field.name}`}
                                        label={field.label}
                                        placeholder={field.defaultText}
                                        rows={2}
                                    />
                                ))}
                            </CollapsibleContent>
                        </Collapsible>
                    </>
                ) : (
                    <FormStringList
                        control={form.control}
                        name="allowedOrigins"
                        label="Allowed origins"
                        description="The widget only loads on these origins. Leave empty to allow any site."
                        addButtonLabel="Add origin"
                        emptyLabel="No origins — the widget can be embedded on any site."
                        defaultValue={{ value: "" }}
                        fieldName={(index) => `allowedOrigins.${index}.value`}
                        itemLabel={(index) => `Origin ${index + 1}`}
                    />
                )}

                {updateMutation.isError && (
                    <Alert variant="destructive">
                        {updateMutation.error instanceof Error
                            ? updateMutation.error.message
                            : "Could not update channel."}
                    </Alert>
                )}
            </div>

            {footer({ isPending: updateMutation.isPending })}
        </form>
    );
}
