import { ReactNode, useEffect, useRef } from "react";

interface NotificationDetailsPreviewProps {
    title: string;
    item: unknown;
    children: ReactNode;
}

export function NotificationDetailsPreview({ title, item, children }: NotificationDetailsPreviewProps) {
    const ref = useRef<HTMLDivElement>(null);

    useEffect(() => {
        ref.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }, [item]);

    return (
        <div ref={ref} className="vstack gap-2 flex-shrink-0">
            <h4 className="mb-0">{title}</h4>
            {children}
        </div>
    );
}
