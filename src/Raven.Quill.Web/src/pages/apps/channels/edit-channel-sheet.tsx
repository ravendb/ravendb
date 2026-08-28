import { useState, type ReactNode } from "react";
import type { ChannelSummaryResponse } from "@/api/generated/server-api";
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
import { GuardedSheet } from "@/components/form/unsaved-changes/guarded-overlays";
import { ChannelConfigForm } from "@/pages/apps/channels/channel-config-form";

type EditChannelSheetProps = {
    slug: string;
    channel: ChannelSummaryResponse;
    /** Omit when the sheet is driven by `open`/`onOpenChange` (e.g. from a dropdown menu item). */
    trigger?: ReactNode;
    open?: boolean;
    onOpenChange?: (open: boolean) => void;
};

export function EditChannelSheet({ slug, channel, trigger, open, onOpenChange }: EditChannelSheetProps) {
    const [uncontrolledOpen, setUncontrolledOpen] = useState(false);
    const isOpen = open ?? uncontrolledOpen;
    const setIsOpen = onOpenChange ?? setUncontrolledOpen;

    return (
        <GuardedSheet open={isOpen} onOpenChange={setIsOpen}>
            {trigger && <SheetTrigger asChild>{trigger}</SheetTrigger>}
            <SheetContent className="w-full gap-0 sm:max-w-lg data-[side=right]:sm:max-w-lg">
                <SheetHeader className="border-b">
                    <SheetTitle>Edit channel</SheetTitle>
                    <SheetDescription>Update “{channel.displayName}”.</SheetDescription>
                </SheetHeader>
                <ChannelConfigForm
                    slug={slug}
                    channel={channel}
                    onSaved={() => setIsOpen(false)}
                    className="flex min-h-0 flex-1 flex-col"
                    bodyClassName="flex-1 overflow-y-auto p-4"
                    footer={({ isPending }) => (
                        <SheetFooter className="flex-row justify-end border-t">
                            <SheetClose asChild>
                                <Button type="button" variant="outline">
                                    Cancel
                                </Button>
                            </SheetClose>
                            <Button type="submit" disabled={isPending}>
                                {isPending && <Spinner />}
                                Save changes
                            </Button>
                        </SheetFooter>
                    )}
                />
            </SheetContent>
        </GuardedSheet>
    );
}
