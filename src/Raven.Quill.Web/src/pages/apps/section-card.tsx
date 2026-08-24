import type { ComponentProps, ReactNode } from "react";
import { cn } from "@/lib/utils";
import { Heading, Text } from "@/components/typography";

export function SectionCard({
    title,
    description,
    action,
    // Draw the section on a raised card instead of flush on the page background. For sections that are
    // a self-contained block of content rather than a heading over a bordered table or list.
    isRaised = false,
    children,
    // Defaults suit a top-level section under a page title. Nested contexts (e.g. a
    // wizard step that already has its own heading) can step the title down a level.
    titleAs = "h2",
    titleVariant = "section",
}: {
    title?: ReactNode;
    description?: ReactNode;
    action?: ReactNode;
    isRaised?: boolean;
    children: ReactNode;
    titleAs?: ComponentProps<typeof Heading>["as"];
    titleVariant?: ComponentProps<typeof Heading>["variant"];
}) {
    return (
        <section className={cn("min-w-0", isRaised && "rounded-md border bg-card p-4")}>
            {(title || action) && (
                <div className="mb-2 flex items-center justify-between gap-3">
                    <div className="space-y-0.5">
                        {title && (
                            <Heading as={titleAs} variant={titleVariant}>
                                {title}
                            </Heading>
                        )}
                        {description && <Text variant="muted">{description}</Text>}
                    </div>
                    {action}
                </div>
            )}
            {children}
        </section>
    );
}
