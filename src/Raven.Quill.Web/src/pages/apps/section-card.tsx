import type { ComponentProps, ReactNode } from "react";
import { cn } from "@/lib/utils";
import { SectionHeader } from "@/components/section-header";

export function SectionCard({
    title,
    count,
    description,
    action,
    // Draw the section on a raised card instead of flush on the page background. For sections that are
    // a self-contained block of content rather than a heading over a bordered table or list.
    isRaised = false,
    children,
    // Defaults suit a top-level section under a page title. Nested contexts (e.g. a
    // wizard step that already has its own heading) can step the title down a level.
    level = "section",
}: {
    title?: ReactNode;
    count?: ReactNode;
    description?: ReactNode;
    action?: ReactNode;
    isRaised?: boolean;
    children: ReactNode;
    level?: ComponentProps<typeof SectionHeader>["level"];
}) {
    return (
        <section className={cn("min-w-0", isRaised && "rounded-md border bg-card p-4")}>
            {(title || count || action) && (
                <SectionHeader
                    className="mb-2"
                    level={level}
                    title={title}
                    count={count}
                    description={description}
                    action={action}
                />
            )}
            {children}
        </section>
    );
}
