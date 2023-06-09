import React, { createContext, useContext, useState } from "react";

interface DirtyFlag {
    isDirty: boolean;
    setIsDirty: (isDirty: boolean) => void;
}

const DirtyFlagContext = createContext<DirtyFlag>(null);

export function DirtyFlagProvider(props: KoToReactDirtyFlag & { children: React.ReactNode }) {
    const { koIsDirty, koSetIsDirty, children } = props;

    const [isDirty, setIsDirty] = useState(koIsDirty());

    const setIsDirtyForKoAndReact = (x: boolean) => {
        koSetIsDirty(x);
        setIsDirty(x);
    };

    return (
        <DirtyFlagContext.Provider value={{ isDirty, setIsDirty: setIsDirtyForKoAndReact }}>
            {children}
        </DirtyFlagContext.Provider>
    );
}

export const useDirtyFlag = () => useContext(DirtyFlagContext);
