import React, { createContext, useContext, useEffect, useMemo, useState } from "react";
import { ExpandedState, OnChangeFn } from "@tanstack/react-table";

// A single global Expand all / Collapse all switch for the analyzer's node-grouped tables. The context
// holds one boolean; each table syncs its own TanStack expansion state to it via useExpandAllSync.

interface ExpandAllContextValue {
    isAllExpanded: boolean;
    toggleAll: () => void;
}

const ExpandAllContext = createContext<ExpandAllContextValue | null>(null);

export function ExpandAllProvider({ children }: { children: React.ReactNode }) {
    const [isAllExpanded, setIsAllExpanded] = useState(false);

    const value = useMemo<ExpandAllContextValue>(
        () => ({ isAllExpanded, toggleAll: () => setIsAllExpanded((v) => !v) }),
        [isAllExpanded]
    );

    return <ExpandAllContext.Provider value={value}>{children}</ExpandAllContext.Provider>;
}

export function useExpandAll(): ExpandAllContextValue {
    const context = useContext(ExpandAllContext);
    if (!context) {
        throw new Error("useExpandAll must be used within an ExpandAllProvider");
    }
    return context;
}

// Drop-in replacement for a table's `useState<ExpandedState>({})`: returns the table's own expansion
// state but keeps it in sync with the global switch. TanStack reads `expanded === true` as "all
// expandable rows open"; getRowCanExpand still gates which rows actually open. Individual row clicks
// update the local state and persist until the next global toggle. Initializing from the current
// global value means a table mounted after a scope switch honors the switch immediately.
export function useExpandAllSync(): [ExpandedState, OnChangeFn<ExpandedState>] {
    const { isAllExpanded } = useExpandAll();
    const [expanded, setExpanded] = useState<ExpandedState>(isAllExpanded ? true : {});

    useEffect(() => {
        setExpanded(isAllExpanded ? true : {});
    }, [isAllExpanded]);

    return [expanded, setExpanded];
}
