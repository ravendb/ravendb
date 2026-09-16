import { useId, useState, type ComponentProps } from "react";
import { Dialog } from "@/components/shadcn/ui/dialog";
import { Sheet } from "@/components/shadcn/ui/sheet";
import { UnsavedChangesConfirm } from "@/components/form/unsaved-changes/unsaved-changes-confirm";
import {
    toChildScopePath,
    UnsavedChangesScopeContext,
    useUnsavedChangesScopePath,
} from "@/components/form/unsaved-changes/unsaved-changes-scope";
import {
    selectHasUnsavedChangesInScope,
    useUnsavedChangesStore,
} from "@/components/form/unsaved-changes/unsaved-changes-store";

// Controlled only: the guard works by withholding onOpenChange, which gates nothing in Radix's uncontrolled mode.
type GuardedOverlayProps = {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    /**
     * For a form living beside the overlay (not inside its content), which the scope lookup cannot
     * see. Prefer forms inside the content; callers using this must also reset the form on close.
     */
    hasUnsavedChanges?: boolean;
};

// Intercepts every Radix close path (X, Escape, overlay click) - closing is not a navigation, so
// the app-level guard never sees it.
function useOverlayCloseGuard(onOpenChange: (open: boolean) => void, hasOwnUnsavedChanges: boolean) {
    const scopeId = useId();
    const scopePath = toChildScopePath(useUnsavedChangesScopePath(), scopeId);
    const hasUnsavedChangesInScope = useUnsavedChangesStore(selectHasUnsavedChangesInScope(scopePath));
    const hasUnsavedChanges = hasOwnUnsavedChanges || hasUnsavedChangesInScope;
    const [isConfirmOpen, setIsConfirmOpen] = useState(false);

    return {
        scopePath,
        isConfirmOpen,
        setIsConfirmOpen,
        requestOpenChange: (isOpen: boolean) => {
            if (!isOpen && hasUnsavedChanges) {
                setIsConfirmOpen(true);
                return;
            }

            onOpenChange(isOpen);
        },
        discardAndClose: () => {
            setIsConfirmOpen(false);
            onOpenChange(false);
        },
    };
}

/** A controlled `Sheet` that confirms before closing over a dirty form inside it. */
export function GuardedSheet({
    children,
    onOpenChange,
    hasUnsavedChanges = false,
    ...props
}: ComponentProps<typeof Sheet> & GuardedOverlayProps) {
    const guard = useOverlayCloseGuard(onOpenChange, hasUnsavedChanges);

    return (
        <UnsavedChangesScopeContext.Provider value={guard.scopePath}>
            <Sheet onOpenChange={guard.requestOpenChange} {...props}>
                {children}
            </Sheet>
            <UnsavedChangesConfirm
                open={guard.isConfirmOpen}
                onOpenChange={guard.setIsConfirmOpen}
                onConfirm={guard.discardAndClose}
            />
        </UnsavedChangesScopeContext.Provider>
    );
}

/** A controlled `Dialog` that confirms before closing over a dirty form inside it. */
export function GuardedDialog({
    children,
    onOpenChange,
    hasUnsavedChanges = false,
    ...props
}: ComponentProps<typeof Dialog> & GuardedOverlayProps) {
    const guard = useOverlayCloseGuard(onOpenChange, hasUnsavedChanges);

    return (
        <UnsavedChangesScopeContext.Provider value={guard.scopePath}>
            <Dialog onOpenChange={guard.requestOpenChange} {...props}>
                {children}
            </Dialog>
            <UnsavedChangesConfirm
                open={guard.isConfirmOpen}
                onOpenChange={guard.setIsConfirmOpen}
                onConfirm={guard.discardAndClose}
            />
        </UnsavedChangesScopeContext.Provider>
    );
}
