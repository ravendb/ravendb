import React, { createContext, useContext, useEffect } from "react";

const DirtyFlagContext = createContext<ReactDirtyFlag>(null);

export function DirtyFlagProvider({ setIsDirty, children }: ReactDirtyFlag & { children: React.ReactNode }) {
    return <DirtyFlagContext.Provider value={{ setIsDirty }}>{children}</DirtyFlagContext.Provider>;
}

export const useDirtyFlag = (isDirty: boolean) => {
    const { setIsDirty } = useContext(DirtyFlagContext);

    useEffect(() => {
        if (isDirty) {
            setIsDirty(true);
            return;
        }

        setIsDirty(false);
    }, [isDirty, setIsDirty]);
};
