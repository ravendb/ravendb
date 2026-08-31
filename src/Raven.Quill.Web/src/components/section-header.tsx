import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { Heading, Text } from "@/components/typography";

// One decision per section header: pick `level` for the role, instead of choosing `as` and
// `variant` independently from the Heading grid. Folds the title + description pairing so the two
// can't drift apart between call sites (the same role always renders the same size).
const LEVELS = {
    section: { as: "h2", variant: "section" },
    subsection: { as: "h3", variant: "subsection" },
} as const;

type Level = keyof typeof LEVELS;

export function SectionHeader({
    level = "section",
    title,
    // A count sits inline next to the title (e.g. a total badge), unlike `action` which is pushed
    // to the far edge for buttons/menus.
    count,
    description,
    action,
    className,
}: {
    level?: Level;
    title?: ReactNode;
    count?: ReactNode;
    description?: ReactNode;
    action?: ReactNode;
    className?: string;
}) {
    const { as, variant } = LEVELS[level];

    return (
        <div className={cn("flex items-start justify-between gap-3", className)}>
            <div className="space-y-0.5">
                {(title || count) && (
                    <div className="flex items-center gap-2">
                        {title && (
                            <Heading as={as} variant={variant}>
                                {title}
                            </Heading>
                        )}
                        {count}
                    </div>
                )}
                {description && <Text variant="muted">{description}</Text>}
            </div>
            {action}
        </div>
    );
}
