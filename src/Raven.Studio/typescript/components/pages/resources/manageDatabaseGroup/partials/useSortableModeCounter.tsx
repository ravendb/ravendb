import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import React, { createContext, useContext, useEffect, useState } from "react";

interface SortableModeCounter {
    counter: number;
    setCounter: React.Dispatch<React.SetStateAction<number>>;
}

const SortableModeCounterContext = createContext<SortableModeCounter>(null);

export function SortableModeCounterProvider({ children }: { children: React.ReactNode }) {
    const [counter, setCounter] = useState(0);

    return (
        <SortableModeCounterContext.Provider value={{ counter, setCounter }}>
            {children}
        </SortableModeCounterContext.Provider>
    );
}

export function useSortableModeCounter(): Pick<SortableModeCounter, "setCounter"> {
    const { counter, setCounter } = useContext(SortableModeCounterContext);
    const { setIsDirty } = useDirtyFlag();

    useEffect(() => {
        if (counter === 0) {
            setIsDirty(false);
            return;
        }

        setIsDirty(true);
    }, [counter, setIsDirty]);

    return {
        setCounter,
    };
}
