import { useResizeObserver } from "components/hooks/useResizeObserver";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { useRef } from "react";

export interface VirtualTableArea {
    ref: React.RefObject<HTMLDivElement>;
    heightInPx: number;
    widthInPx: number;
    bodyWidthInPx: number;
}

interface UseVirtualTableAreaProps {
    heightInPx?: number;
    widthInPx?: number;
}

// Measures the space the table is rendered in. Create it before the columns, which need the width,
// and before useLazyVirtualTable, which turns the height into a page size.
export function useVirtualTableArea({ heightInPx, widthInPx }: UseVirtualTableAreaProps = {}): VirtualTableArea {
    const ref = useRef<HTMLDivElement>(null);
    const measured = useResizeObserver({ ref });

    const width = widthInPx ?? measured.width ?? 0;

    return {
        ref,
        heightInPx: heightInPx ?? measured.height ?? virtualTableConstants.defaultTableHeightInPx,
        widthInPx: width,
        bodyWidthInPx: virtualTableUtils.getTableBodyWidth(width),
    };
}
