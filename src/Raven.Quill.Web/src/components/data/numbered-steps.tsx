import type { ReactNode } from "react";
import { Heading, Text } from "@/components/typography";

export type NumberedStep = {
    title: string;
    content: ReactNode;
};

// A numbered walkthrough: a badge and a connector rail in the left column, the step's title and content
// in the right. The rail is drawn on every step but the last so the steps read as a single sequence.
export function NumberedSteps({ steps }: { steps: NumberedStep[] }) {
    return (
        <ol className="grid gap-0">
            {steps.map((step, index) => {
                const isLast = index === steps.length - 1;

                return (
                    <li key={step.title} className="grid grid-cols-[1.5rem_1fr] gap-x-3">
                        <div className="flex flex-col items-center">
                            <Text
                                variant="caption"
                                as="span"
                                className="flex size-6 items-center justify-center rounded-full border border-border bg-muted font-medium"
                            >
                                {index + 1}
                            </Text>
                            {!isLast && <span aria-hidden="true" className="mt-1 w-px flex-1 bg-border" />}
                        </div>
                        <div className={isLast ? "" : "pb-5"}>
                            <Heading as="h3" variant="subsection" className="mb-1.5">
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
