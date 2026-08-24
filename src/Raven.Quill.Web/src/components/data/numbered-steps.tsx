import type { ReactNode } from "react";

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
                            <span className="flex size-6 items-center justify-center rounded-full border border-border bg-muted text-xs font-medium text-muted-foreground">
                                {index + 1}
                            </span>
                            {!isLast && <span aria-hidden="true" className="mt-1 w-px flex-1 bg-border" />}
                        </div>
                        <div className={isLast ? "" : "pb-5"}>
                            <h3 className="mb-1.5 text-base font-medium">{step.title}</h3>
                            {step.content}
                        </div>
                    </li>
                );
            })}
        </ol>
    );
}
