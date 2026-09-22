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
            description={
                <>
                    Your changes haven’t been saved.
                    <br />
                    Discarding them cannot be undone.
                </>
            }
            confirmLabel="Discard changes"
            cancelLabel="Keep editing"
        />
    );
}
