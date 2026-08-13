import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useWatch } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import {
    SheetClose,
    SheetContent,
    SheetDescription,
    SheetFooter,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/shadcn/ui/sheet";
import { ChevronDown } from "lucide-react";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { FormInput } from "@/components/form/form-input";
import { FormStringList } from "@/components/form/form-string-list";
import { FormSwitch } from "@/components/form/form-switch";
import { FormTextarea } from "@/components/form/form-textarea";
import { GuardedSheet } from "@/components/form/unsaved-changes/guarded-overlays";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { withNestedSubmit } from "@/lib/form-utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";
import {
    TELEGRAM_MESSAGE_FIELDS,
    telegramMessagesSchema,
    toMessagesDto,
    toMessagesFormValues,
} from "@/pages/apps/channels/telegram-message-defaults";

type EditChannelSheetProps = {
    slug: string;
    channel: ChannelSummaryResponse;
    trigger: ReactNode;
};

export function EditChannelSheet({ slug, channel, trigger }: EditChannelSheetProps) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <GuardedSheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>{trigger}</SheetTrigger>
            <SheetContent className="w-full gap-0 sm:max-w-lg data-[side=right]:sm:max-w-lg">
                <SheetHeader className="border-b">
                    <SheetTitle>Edit channel</SheetTitle>
                    <SheetDescription>Update “{channel.displayName}”.</SheetDescription>
                </SheetHeader>
                <EditChannelForm slug={slug} channel={channel} onSaved={() => setIsOpen(false)} />
            </SheetContent>
        </GuardedSheet>
    );
}

const editChannelSchema = z.object({
    displayName: z.string().trim().min(1, "Channel name is required"),
    enabled: z.boolean(),
    shouldReplaceAllowedOrigins: z.boolean(),
    allowedOrigins: z.array(z.object({ value: z.string().trim() })),
    botToken: z.string().trim(),
    messages: telegramMessagesSchema,
});

type EditChannelFormData = z.infer<typeof editChannelSchema>;

function EditChannelForm({
    slug,
    channel,
    onSaved,
}: {
    slug: string;
    channel: ChannelSummaryResponse;
    onSaved: () => void;
}) {
    const queryClient = useQueryClient();

    const isTelegram = channel.type === "Telegram";
    const [areMessagesOpen, setAreMessagesOpen] = useState(false);

    const form = useForm<EditChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(editChannelSchema),
        defaultValues: {
            displayName: channel.displayName,
            enabled: channel.enabled,
            shouldReplaceAllowedOrigins: false,
            allowedOrigins: [],
            botToken: "",
            messages: toMessagesFormValues(channel.telegram?.messages),
        },
    });

    const shouldReplaceAllowedOrigins = useWatch({ control: form.control, name: "shouldReplaceAllowedOrigins" });
    const unsavedChanges = useFormUnsavedChanges(form);

    const updateMutation = useMutation({
        mutationFn: (values: EditChannelFormData) =>
            // Update is a partial edit: null fields are left unchanged on the server.
            api.services.channels.update(slug, channel.channelId, {
                displayName: values.displayName.trim(),
                allowedOrigins:
                    !isTelegram && values.shouldReplaceAllowedOrigins
                        ? values.allowedOrigins.map((origin) => origin.value.trim()).filter(Boolean)
                        : null,
                enabled: values.enabled,
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
            onSaved();
        },
    });

    return (
        <form
            className="flex min-h-0 flex-1 flex-col"
            onSubmit={withNestedSubmit(form.handleSubmit((values) => updateMutation.mutate(values)))}
        >
            <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                <FormInput
                    control={form.control}
                    name="displayName"
                    label="Channel name"
                    placeholder="e.g. Storefront help"
                    description="Shown in the channels list."
                />
                <FormSwitch control={form.control} name="enabled" label="Enabled" />
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
                    <>
                        <div className="flex flex-col gap-1.5">
                            <FormSwitch
                                control={form.control}
                                name="shouldReplaceAllowedOrigins"
                                label="Replace allowed origins"
                            />
                            <p className="text-xs text-muted-foreground">
                                The current origins are not shown here. Leave this off to keep them, or turn it on to
                                replace the whole list.
                            </p>
                        </div>
                        {shouldReplaceAllowedOrigins && (
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
                    </>
                )}

                {updateMutation.isError && (
                    <Alert variant="destructive">
                        {updateMutation.error instanceof Error
                            ? updateMutation.error.message
                            : "Could not update channel."}
                    </Alert>
                )}
            </div>

            <SheetFooter className="flex-row justify-end border-t">
                <SheetClose asChild>
                    <Button type="button" variant="outline">
                        Cancel
                    </Button>
                </SheetClose>
                <Button type="submit" disabled={updateMutation.isPending}>
                    {updateMutation.isPending && <Spinner />}
                    Save changes
                </Button>
            </SheetFooter>
        </form>
    );
}
