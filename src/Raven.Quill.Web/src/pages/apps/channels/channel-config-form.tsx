import { useId, useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { ChannelSummaryResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { Switch } from "@/components/shadcn/ui/switch";
import { FormInput } from "@/components/form/form-input";
import { FormStringList } from "@/components/form/form-string-list";
import {
    DISCORD_BOT_TOKEN_FORMAT,
    SLACK_BOT_TOKEN_FORMAT,
    rotatedTokenField,
} from "@/pages/apps/channels/channel-token-fields";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { withNestedSubmit } from "@/lib/form-utils";
import { cn } from "@/lib/utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";

const editChannelSchema = z.object({
    displayName: z.string().trim().min(1, "Channel name is required"),
    allowedOrigins: z.array(z.object({ value: z.string().trim() })),
    botToken: z.string().trim(),
    slackBotToken: rotatedTokenField(SLACK_BOT_TOKEN_FORMAT),
    slackSigningSecret: z.string().trim(),
    discordBotToken: rotatedTokenField(DISCORD_BOT_TOKEN_FORMAT),
});

type EditChannelFormData = z.infer<typeof editChannelSchema>;

// The channel's editable settings, rendered inside the edit sheet. The caller owns the surrounding
// chrome by supplying container classes and the footer (which receives the pending state so it can
// disable/spin its submit button). The visible fields depend on the channel type: web widgets edit
// their allowed origins, Telegram bots rotate their token (their canned messages live in the detail's
// Bot messages tab).
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
    const rotateTokenId = useId();

    const isTelegram = channel.type === "Telegram";
    const isSlack = channel.type === "Slack";
    const isDiscord = channel.type === "Discord";
    const isIFrame = channel.type === "IFrame";
    // Rotating the token is off by default: the field only appears once the switch is on, so the token
    // input can't be mistaken for a required field that must be filled to save other changes.
    const [isRotatingToken, setIsRotatingToken] = useState(false);

    const form = useForm<EditChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(editChannelSchema),
        defaultValues: {
            displayName: channel.displayName,
            allowedOrigins: (channel.allowedOrigins ?? []).map((value) => ({ value })),
            botToken: "",
            slackBotToken: "",
            slackSigningSecret: "",
            discordBotToken: "",
        },
    });

    const unsavedChanges = useFormUnsavedChanges(form);

    const onRotateToggle = (checked: boolean) => {
        setIsRotatingToken(checked);
        // Turning the switch back off discards any typed token so nothing rotates on save.
        if (!checked) {
            form.resetField("botToken");
            form.resetField("slackBotToken");
            form.resetField("slackSigningSecret");
            form.resetField("discordBotToken");
        }
    };

    const updateMutation = useMutation({
        mutationFn: (values: EditChannelFormData) =>
            // Update is a partial edit: null fields are left unchanged on the server.
            api.services.channels.update(slug, channel.channelId, {
                displayName: values.displayName.trim(),
                allowedOrigins: isIFrame
                    ? values.allowedOrigins.map((origin) => origin.value.trim()).filter(Boolean)
                    : null,
                // Enabled state is owned by the header's Pause/Resume toggle; null leaves it untouched.
                enabled: null,
                telegram: isTelegram ? { botToken: values.botToken.trim() || null } : null,
                slack: isSlack
                    ? {
                          botToken: values.slackBotToken.trim() || null,
                          signingSecret: values.slackSigningSecret.trim() || null,
                      }
                    : null,
                discord: isDiscord ? { botToken: values.discordBotToken.trim() || null } : null,
            }),
        onSuccess: async () => {
            unsavedChanges.markSaved();
            await invalidateChannelQueries(queryClient, slug, channel.type);
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
                    <div className="flex flex-col gap-3">
                        <div className="space-y-0.5">
                            <div className="flex items-center gap-2">
                                <FieldLabel htmlFor={rotateTokenId}>Rotate bot token</FieldLabel>
                                <Switch id={rotateTokenId} checked={isRotatingToken} onCheckedChange={onRotateToggle} />
                            </div>
                            <FieldDescription>
                                Turn on to replace the current token with a new one from @BotFather. The current token
                                is never shown.
                            </FieldDescription>
                        </div>
                        {isRotatingToken && (
                            <FormInput
                                control={form.control}
                                name="botToken"
                                type="password"
                                label="New bot token"
                                placeholder="Paste the new token from @BotFather"
                            />
                        )}
                    </div>
                ) : isSlack ? (
                    <div className="flex flex-col gap-3">
                        <div className="space-y-0.5">
                            <div className="flex items-center gap-2">
                                <FieldLabel htmlFor={rotateTokenId}>Rotate credentials</FieldLabel>
                                <Switch id={rotateTokenId} checked={isRotatingToken} onCheckedChange={onRotateToggle} />
                            </div>
                            <FieldDescription>
                                Turn on to replace the Slack app’s bot token, signing secret, or both. Leave a field
                                empty to keep the current value; neither is ever shown.
                            </FieldDescription>
                        </div>
                        {isRotatingToken && (
                            <>
                                <FormInput
                                    control={form.control}
                                    name="slackBotToken"
                                    type="password"
                                    label="New bot token"
                                    placeholder="xoxb-..."
                                    description="Must belong to the same workspace and bot as the current token."
                                />
                                <FormInput
                                    control={form.control}
                                    name="slackSigningSecret"
                                    type="password"
                                    label="New signing secret"
                                    placeholder="From Basic Information > App Credentials"
                                    description="Must match the Slack app’s signing secret or event deliveries stop verifying."
                                />
                            </>
                        )}
                    </div>
                ) : isDiscord ? (
                    <div className="flex flex-col gap-3">
                        <div className="space-y-0.5">
                            <div className="flex items-center gap-2">
                                <FieldLabel htmlFor={rotateTokenId}>Rotate bot token</FieldLabel>
                                <Switch id={rotateTokenId} checked={isRotatingToken} onCheckedChange={onRotateToggle} />
                            </div>
                            <FieldDescription>
                                Turn on to replace the current token with a freshly reset one from the app’s Bot page.
                                The current token is never shown.
                            </FieldDescription>
                        </div>
                        {isRotatingToken && (
                            <FormInput
                                control={form.control}
                                name="discordBotToken"
                                type="password"
                                label="New bot token"
                                placeholder="Paste the reset token from the app’s Bot page"
                                description="Must belong to the same Discord bot as the current token."
                            />
                        )}
                    </div>
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
