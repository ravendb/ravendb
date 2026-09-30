import { useEffect, useRef, type ReactNode } from "react";
import { useBlocker, type Blocker } from "react-router";
import { UnsavedChangesConfirm } from "@/components/form/unsaved-changes/unsaved-changes-confirm";
import {
    selectHasUnsavedChanges,
    useUnsavedChangesStore,
} from "@/components/form/unsaved-changes/unsaved-changes-store";

/** Blocks navigation and page unload while any registered form is dirty. Mounted once - React Router consults only one blocker. */
export function UnsavedChangesProvider({ children }: { children: ReactNode }) {
    const hasUnsavedChanges = useUnsavedChangesStore(selectHasUnsavedChanges);

    // Search-only changes (filters, a wizard's preselected step) stay on the same screen.
    const blocker = useBlocker(
        ({ currentLocation, nextLocation }) => hasUnsavedChanges && currentLocation.pathname !== nextLocation.pathname,
    );

    useEffect(() => {
        if (!hasUnsavedChanges) {
            return;
        }

        // Registered only while there is something to lose, so the page stays bfcache-eligible otherwise.
        const confirmUnload = (event: BeforeUnloadEvent) => event.preventDefault();
        window.addEventListener("beforeunload", confirmUnload);
        return () => window.removeEventListener("beforeunload", confirmUnload);
    }, [hasUnsavedChanges]);

    return (
        <>
            {children}
            <UnsavedChangesPrompt blocker={blocker} />
        </>
    );
}

function UnsavedChangesPrompt({ blocker }: { blocker: Blocker }) {
    // After proceed() the dialog still reports its own close; resetting the blocker then would re-block.
    const isProceedingRef = useRef(false);

    return (
        <UnsavedChangesConfirm
            open={blocker.state === "blocked"}
            onOpenChange={(isOpen) => {
                if (isOpen) {
                    return;
                }

                if (isProceedingRef.current) {
                    isProceedingRef.current = false;
                    return;
                }

                blocker.reset?.();
            }}
            onConfirm={() => {
                isProceedingRef.current = true;
                blocker.proceed?.();
            }}
        />
    );
}
