import type { ReactNode } from "react";
import { Heading, Text } from "@/components/typography";
import { cn } from "@/lib/utils";

export type NumberedStep = {
    title: string;
    content: ReactNode;
};

// "sm" tightens the badge and title for dense contexts (e.g. a collapsible helper inside a form sheet);
// "default" is the roomier walkthrough used on its own.
type NumberedStepsSize = "default" | "sm";

const SIZE_CLASSES: Record<NumberedStepsSize, { column: string; badge: string; title: "subsection" | "label" }> = {
    default: { column: "grid-cols-[1.5rem_1fr]", badge: "size-6", title: "subsection" },
    sm: { column: "grid-cols-[1.25rem_1fr]", badge: "size-5 text-[0.6875rem]", title: "label" },
};

// A numbered walkthrough: a badge and a connector rail in the left column, the step's title and content
// in the right. The rail is drawn on every step but the last so the steps read as a single sequence.
export function NumberedSteps({ steps, size = "default" }: { steps: NumberedStep[]; size?: NumberedStepsSize }) {
    const sizeClasses = SIZE_CLASSES[size];

    return (
        <ol className="grid gap-0">
            {steps.map((step, index) => {
                const isLast = index === steps.length - 1;

                return (
                    <li key={step.title} className={cn("grid gap-x-3", sizeClasses.column)}>
                        <div className="flex flex-col items-center">
                            <Text
                                variant="caption"
                                as="span"
                                className={cn(
                                    "flex items-center justify-center rounded-full border border-border bg-muted font-medium",
                                    sizeClasses.badge,
                                )}
                            >
                                {index + 1}
                            </Text>
                            {!isLast && <span aria-hidden="true" className="w-px flex-1 bg-border" />}
                        </div>
                        <div className={isLast ? "" : "pb-5"}>
                            <Heading as="h3" variant={sizeClasses.title} className="mb-1">
                                {step.title}
                            </Heading>
                            {step.content}
                        </div>
                    </li>
                );
            })}
        </ol>
    );
}
