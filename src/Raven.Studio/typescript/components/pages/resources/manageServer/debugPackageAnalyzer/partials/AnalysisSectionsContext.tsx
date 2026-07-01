import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";

export interface AnalysisSectionEntry {
    id: string;
    label: string;
    // optional heading the section belongs to; consecutive sections sharing a group are labelled in the nav rail
    group?: string;
}

interface RegisteredSection extends AnalysisSectionEntry {
    element: HTMLElement;
}

interface AnalysisSectionsContextValue {
    register: (section: RegisteredSection) => void;
    unregister: (id: string) => void;
    entries: AnalysisSectionEntry[];
}

const AnalysisSectionsContext = createContext<AnalysisSectionsContextValue | null>(null);

function sortByDomOrder(sections: RegisteredSection[]): RegisteredSection[] {
    return [...sections].sort((a, b) => {
        if (a.element === b.element) {
            return 0;
        }
        const position = a.element.compareDocumentPosition(b.element);
        if (position & Node.DOCUMENT_POSITION_FOLLOWING) {
            return -1;
        }
        if (position & Node.DOCUMENT_POSITION_PRECEDING) {
            return 1;
        }
        return 0;
    });
}

export function AnalysisSectionsProvider({ children }: { children: React.ReactNode }) {
    const sectionsRef = useRef<Map<string, RegisteredSection>>(new Map());
    const [version, setVersion] = useState(0);

    const register = useCallback((section: RegisteredSection) => {
        sectionsRef.current.set(section.id, section);
        setVersion((v) => v + 1);
    }, []);

    const unregister = useCallback((id: string) => {
        if (sectionsRef.current.delete(id)) {
            setVersion((v) => v + 1);
        }
    }, []);

    const entries = useMemo(
        () => sortByDomOrder([...sectionsRef.current.values()]).map(({ id, label, group }) => ({ id, label, group })),
        // recompute whenever the registry changes
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [version]
    );

    const value = useMemo<AnalysisSectionsContextValue>(
        () => ({ register, unregister, entries }),
        [register, unregister, entries]
    );

    return <AnalysisSectionsContext.Provider value={value}>{children}</AnalysisSectionsContext.Provider>;
}

export function useAnalysisSections(): AnalysisSectionsContextValue {
    const context = useContext(AnalysisSectionsContext);
    if (!context) {
        throw new Error("useAnalysisSections must be used within an AnalysisSectionsProvider");
    }
    return context;
}
