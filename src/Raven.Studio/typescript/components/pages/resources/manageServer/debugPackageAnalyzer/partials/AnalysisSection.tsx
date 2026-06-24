import React, { useEffect, useRef } from "react";
import classNames from "classnames";
import { useAnalysisSections } from "./AnalysisSectionsContext";

interface AnalysisSectionProps {
    id: string;
    label: string;
    group?: string;
    className?: string;
    children: React.ReactNode;
}

// Wraps a page section, giving it a stable anchor id and registering it in the sections registry
// while it actually has rendered content. A MutationObserver re-evaluates when async children mount.
export default function AnalysisSection({ id, label, group, className, children }: AnalysisSectionProps) {
    const ref = useRef<HTMLDivElement>(null);
    const { register, unregister } = useAnalysisSections();

    useEffect(() => {
        const element = ref.current;
        if (!element) {
            return undefined;
        }

        const sync = () => {
            if (element.childElementCount > 0) {
                register({ id, label, group, element });
            } else {
                unregister(id);
            }
        };

        sync();
        const observer = new MutationObserver(sync);
        observer.observe(element, { childList: true });

        return () => {
            observer.disconnect();
            unregister(id);
        };
    }, [id, label, group, register, unregister]);

    return (
        <div id={id} ref={ref} className={classNames("analysis-section", className)}>
            {children}
        </div>
    );
}
