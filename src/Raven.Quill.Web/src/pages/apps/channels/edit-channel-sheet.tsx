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
import { FormInput } from "@/components/form/form-input";
import { FormStringList } from "@/components/form/form-string-list";
import { FormSwitch } from "@/components/form/form-switch";
import { GuardedSheet } from "@/components/form/unsaved-changes/guarded-overlays";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { withNestedSubmit } from "@/lib/form-utils";
import { invalidateChannelQueries } from "@/lib/query-invalidation";

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

    const form = useForm<EditChannelFormData>({
        mode: "onChange",
        resolver: zodResolver(editChannelSchema),
        defaultValues: {
            displayName: channel.displayName,
            enabled: channel.enabled,
            shouldReplaceAllowedOrigins: false,
            allowedOrigins: [],
        },
    });

    const shouldReplaceAllowedOrigins = useWatch({ control: form.control, name: "shouldReplaceAllowedOrigins" });
    const unsavedChanges = useFormUnsavedChanges(form);

    const updateMutation = useMutation({
        mutationFn: (values: EditChannelFormData) =>
            // Update is a partial edit: null fields are left unchanged on the server.
            api.services.channels.update(slug, channel.channelId, {
                displayName: values.displayName.trim(),
                allowedOrigins: values.shouldReplaceAllowedOrigins
                    ? values.allowedOrigins.map((origin) => origin.value.trim()).filter(Boolean)
                    : null,
                enabled: values.enabled,
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
                <div className="flex flex-col gap-1.5">
                    <FormSwitch
                        control={form.control}
                        name="shouldReplaceAllowedOrigins"
                        label="Replace allowed origins"
                    />
                    <p className="text-xs text-muted-foreground">
                        The current origins are not shown here. Leave this off to keep them, or turn it on to replace
                        the whole list.
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
