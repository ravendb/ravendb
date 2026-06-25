import { useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";

type ExpandableTextProps = {
    children: ReactNode;
    maxLines?: number;
    className?: string;
};

export function ExpandableText({ children, maxLines = 3, className }: ExpandableTextProps) {
    const contentRef = useRef<HTMLDivElement>(null);
    const [isExpanded, setIsExpanded] = useState(false);
    const [canExpand, setCanExpand] = useState(false);

    // Overflow can only be known after layout. Measure before the browser paints so the
    // toggle is present in the first frame instead of flashing in late, and re-check on
    // resize. Skip while expanded — nothing is clamped then, and the toggle stays visible
    // because it was the user who expanded it.
    useLayoutEffect(() => {
        const element = contentRef.current;
        if (!element || isExpanded) {
            return;
        }

        const measure = () => setCanExpand(element.scrollHeight > element.clientHeight + 1);
        measure();

        const observer = new ResizeObserver(measure);
        observer.observe(element);
        return () => observer.disconnect();
    }, [children, maxLines, isExpanded]);

    const clampStyle: CSSProperties | undefined = isExpanded
        ? undefined
        : { display: "-webkit-box", WebkitBoxOrient: "vertical", WebkitLineClamp: maxLines, overflow: "hidden" };

    return (
        <div className="flex flex-col items-end">
            <div ref={contentRef} style={clampStyle} className={className}>
                {children}
            </div>
            {(canExpand || isExpanded) && (
                <Button
                    variant="link"
                    className="mt-1 h-auto gap-1 p-0 text-xs font-medium"
                    onClick={() => setIsExpanded((expanded) => !expanded)}
                    aria-expanded={isExpanded}
                >
                    {isExpanded ? (
                        <>
                            Show less
                            <ChevronUp />
                        </>
                    ) : (
                        <>
                            Show more
                            <ChevronDown />
                        </>
                    )}
                </Button>
            )}
        </div>
    );
}
