import React, { createContext, useContext, useEffect } from "react";

const DirtyFlagContext = createContext<ReactDirtyFlag>(null);

export function DirtyFlagProvider({ setIsDirty, children }: ReactDirtyFlag & { children: React.ReactNode }) {
    return <DirtyFlagContext.Provider value={{ setIsDirty }}>{children}</DirtyFlagContext.Provider>;
}

export const useDirtyFlag = (isDirty: boolean, customDialog?: () => JQueryDeferred<confirmDialogResult>) => {
    const { setIsDirty } = useContext(DirtyFlagContext);

    useEffect(() => {
        if (isDirty) {
            setIsDirty(true, customDialog);
            return;
        }

        setIsDirty(false, null);
    }, [isDirty, customDialog, setIsDirty]);
};
