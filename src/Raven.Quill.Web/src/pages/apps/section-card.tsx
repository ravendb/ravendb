import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function SectionCard({
    title,
    description,
    action,
    // Draw the section on a raised card instead of flush on the page background. For sections that are
    // a self-contained block of content rather than a heading over a bordered table or list.
    isRaised = false,
    children,
}: {
    title?: ReactNode;
    description?: ReactNode;
    action?: ReactNode;
    isRaised?: boolean;
    children: ReactNode;
}) {
    return (
        <section className={cn("min-w-0", isRaised && "rounded-md border bg-card p-4")}>
            {(title || action) && (
                <div className="mb-2 flex items-center justify-between gap-3">
                    <div className="space-y-0.5">
                        {title && <h2 className="text-lg font-semibold tracking-tight">{title}</h2>}
                        {description && <p className="text-sm text-muted-foreground">{description}</p>}
                    </div>
                    {action}
                </div>
            )}
            {children}
        </section>
    );
}
