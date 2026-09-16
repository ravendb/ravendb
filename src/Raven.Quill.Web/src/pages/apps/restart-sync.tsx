import { RotateCw } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { DropdownMenuItem } from "@/components/shadcn/ui/dropdown-menu";
import { Spinner } from "@/components/shadcn/ui/spinner";

export function RestartSyncButton({ isRestarting, onClick }: { isRestarting: boolean; onClick: () => void }) {
    return (
        <Button size="sm" onClick={onClick} disabled={isRestarting}>
            <RestartSyncLabel isRestarting={isRestarting} />
        </Button>
    );
}

export function RestartSyncMenuItem({ isRestarting, onSelect }: { isRestarting: boolean; onSelect: () => void }) {
    return (
        <DropdownMenuItem disabled={isRestarting} onSelect={onSelect}>
            <RestartSyncLabel isRestarting={isRestarting} />
        </DropdownMenuItem>
    );
}

function RestartSyncLabel({ isRestarting }: { isRestarting: boolean }) {
    if (isRestarting) {
        return (
            <>
                <Spinner />
                Restarting...
            </>
        );
    }

    return (
        <>
            <RotateCw aria-hidden="true" />
            Restart sync
        </>
    );
}

export function RestartSyncDialog({
    open,
    onOpenChange,
    onConfirm,
}: {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    onConfirm: () => void;
}) {
    return (
        <ConfirmDialog
            open={open}
            onOpenChange={onOpenChange}
            title="Restart sync?"
            description="Syncing stops and starts again from the last position it saved. Nothing already synced is lost, but new changes can take a moment to arrive while the source database reconnects."
            confirmLabel="Restart"
            onConfirm={onConfirm}
        />
    );
}
