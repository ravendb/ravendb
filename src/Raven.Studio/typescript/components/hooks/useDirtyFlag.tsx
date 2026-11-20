import React, { createContext, useCallback, useContext, useEffect, useId, useMemo, useRef } from "react";

type CustomDialog = () => JQueryDeferred<confirmDialogResult>;

interface DirtyFlagRegistry {
    setIsDirty: (id: string, isDirty: boolean, customDialog?: CustomDialog) => void;
    isDirty: (id: string) => boolean;
}

const DirtyFlagContext = createContext<DirtyFlagRegistry>(null);

export function DirtyFlagProvider({ setIsDirty, children }: ReactDirtyFlag & { children: React.ReactNode }) {
    const dirtyEntriesRef = useRef(new Map<string, CustomDialog>());

    const setEntry = useCallback(
        (id: string, isDirty: boolean, customDialog?: CustomDialog) => {
            const dirtyEntries = dirtyEntriesRef.current;

            if (isDirty) {
                dirtyEntries.set(id, customDialog);
            } else {
                dirtyEntries.delete(id);
            }

            if (dirtyEntries.size === 0) {
                setIsDirty(false, null);
                return;
            }

            setIsDirty(true, Array.from(dirtyEntries.values()).find(Boolean));
        },
        [setIsDirty]
    );

    const registry = useMemo<DirtyFlagRegistry>(
        () => ({ setIsDirty: setEntry, isDirty: (id) => dirtyEntriesRef.current.has(id) }),
        [setEntry]
    );

    return <DirtyFlagContext.Provider value={registry}>{children}</DirtyFlagContext.Provider>;
}

export const useDirtyFlagRegistry = () => useContext(DirtyFlagContext);

export const useDirtyFlag = (isDirty: boolean, customDialog?: CustomDialog) => {
    const id = useId();
    const registry = useDirtyFlagRegistry();

    useEffect(() => {
        registry.setIsDirty(id, isDirty, customDialog);
    }, [id, isDirty, customDialog, registry]);

    useEffect(() => () => registry.setIsDirty(id, false), [id, registry]);

    const setIsDirty = useCallback(
        (isDirty: boolean, customDialog?: CustomDialog) => registry.setIsDirty(id, isDirty, customDialog),
        [id, registry]
    );

    return { id, setIsDirty };
};
