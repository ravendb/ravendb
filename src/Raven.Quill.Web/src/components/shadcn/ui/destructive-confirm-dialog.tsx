import { useId, useState, type ReactNode } from "react";

import { InlineCode } from "@/components/data/inline-code";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Input } from "@/components/shadcn/ui/input";
import { Label } from "@/components/shadcn/ui/label";
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

type DestructiveConfirmDialogProps = {
    /** Optional: omit when the dialog is opened programmatically via `isOpen`/`onOpenChange`. */
    trigger?: ReactNode;
    title: ReactNode;
    description: ReactNode;
    confirmLabel: string;
    isOpen: boolean;
    onOpenChange: (open: boolean) => void;
    onConfirm: () => void;
    isPending: boolean;
    /** Rendered inside a destructive alert when the action failed. */
    error?: ReactNode;
    /**
     * Resource name the operator has to retype before confirming, used on cascading or
     * otherwise unrecoverable actions. Omit it for a single-click confirmation.
     */
    confirmationText?: string;
};

export function DestructiveConfirmDialog({
    trigger,
    title,
    description,
    confirmLabel,
    isOpen,
    onOpenChange,
    onConfirm,
    isPending,
    error,
    confirmationText,
}: DestructiveConfirmDialogProps) {
    return (
        <Dialog open={isOpen} onOpenChange={onOpenChange}>
            {trigger && <DialogTrigger asChild>{trigger}</DialogTrigger>}
            <DialogContent>
                {/* Closing unmounts the content, which resets the typed confirmation, so
                    reopening can never reuse a gate satisfied for an earlier attempt. */}
                <DestructiveConfirmForm
                    title={title}
                    description={description}
                    confirmLabel={confirmLabel}
                    onConfirm={onConfirm}
                    isPending={isPending}
                    error={error}
                    confirmationText={confirmationText}
                />
            </DialogContent>
        </Dialog>
    );
}

type DestructiveConfirmFormProps = Pick<
    DestructiveConfirmDialogProps,
    "title" | "description" | "confirmLabel" | "onConfirm" | "isPending" | "error" | "confirmationText"
>;

function DestructiveConfirmForm({
    title,
    description,
    confirmLabel,
    onConfirm,
    isPending,
    error,
    confirmationText,
}: DestructiveConfirmFormProps) {
    const confirmationInputId = useId();
    const [typedConfirmation, setTypedConfirmation] = useState("");

    const isConfirmed = confirmationText === undefined || typedConfirmation.trim() === confirmationText.trim();

    return (
        <form
            className="grid gap-4"
            onSubmit={(event) => {
                event.preventDefault();
                onConfirm();
            }}
        >
            <DialogHeader>
                <DialogTitle>{title}</DialogTitle>
                <DialogDescription>{description}</DialogDescription>
            </DialogHeader>

            {confirmationText !== undefined && (
                <div className="grid gap-2">
                    {/* `block` over Label's default flex row, so the name reads inline in the
                        sentence instead of becoming a gapped flex item. Label's `leading-none`
                        has to go with it: the inline name chip has vertical padding, which
                        doesn't grow a line box, so it overflows a font-size-tall one. */}
                    <Label htmlFor={confirmationInputId} className="block leading-normal font-normal">
                        Type <InlineCode>{confirmationText}</InlineCode> to confirm
                    </Label>
                    <Input
                        id={confirmationInputId}
                        value={typedConfirmation}
                        onChange={(event) => setTypedConfirmation(event.target.value)}
                        autoComplete="off"
                        spellCheck={false}
                    />
                </div>
            )}

            {error != null && <Alert variant="destructive">{error}</Alert>}

            <DialogFooter>
                <DialogClose asChild>
                    <Button type="button" variant="outline">
                        Cancel
                    </Button>
                </DialogClose>
                <Button type="submit" variant="destructive" disabled={isPending || !isConfirmed}>
                    {isPending && <Spinner />}
                    {confirmLabel}
                </Button>
            </DialogFooter>
        </form>
    );
}
