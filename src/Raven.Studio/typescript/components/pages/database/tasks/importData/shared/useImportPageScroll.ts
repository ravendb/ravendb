import { RefObject, useEffect, useRef, useState } from "react";
import { useScrollSpy } from "components/hooks/useScrollSpy";
import { getSectionIds, SectionNavItem } from "components/pages/database/tasks/importData/shared/importNav";

export interface ImportPageScroll {
    contentRef: RefObject<HTMLDivElement>;
    activeSectionId: string | null;
    selectSection: (id: string) => void;
}

export function useImportPageScroll(navItems: SectionNavItem[]): ImportPageScroll {
    const contentRef = useRef<HTMLDivElement>(null);
    const [scrollRoot, setScrollRoot] = useState<Element | null>(null);
    // the scroll container exists only after the first render, and useScrollSpy needs it as state to re-observe
    useEffect(() => {
        setScrollRoot(contentRef.current);
    }, []);

    const { activeId, selectSection } = useScrollSpy(getSectionIds(navItems), { root: scrollRoot });

    return { contentRef, activeSectionId: activeId, selectSection };
}
