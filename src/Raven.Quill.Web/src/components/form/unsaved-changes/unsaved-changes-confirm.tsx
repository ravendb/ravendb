import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";

type UnsavedChangesConfirmProps = {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    onConfirm: () => void;
};

export function UnsavedChangesConfirm({ open, onOpenChange, onConfirm }: UnsavedChangesConfirmProps) {
    return (
        <ConfirmDialog
            open={open}
            onOpenChange={onOpenChange}
            onConfirm={onConfirm}
            variant="warning"
            title="Discard unsaved changes?"
            description="What you filled in here has not been saved yet. Leaving now discards it."
            confirmLabel="Discard changes"
            cancelLabel="Keep editing"
        />
    );
}
