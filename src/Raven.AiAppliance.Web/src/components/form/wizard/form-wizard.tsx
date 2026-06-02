import { useState, type JSX } from "react";
import { Button } from "@/components/shadcn/ui/button";
import { useFormContext } from "react-hook-form";
import { cn } from "@/lib/utils";
import { ArrowRight, Check } from "lucide-react";
import { Spinner } from "@/components/shadcn/ui/spinner";

export type WizardFooterComponentProps<StepId extends string> =
    | {
          stepPosition: "first";
          currentStep: WizardStep<StepId>;
          cancel: () => void;
          nextStep: () => void;
      }
    | {
          stepPosition: "middle";
          currentStep: WizardStep<StepId>;
          cancel: () => void;
          nextStep: () => void;
          prevStep: () => void;
      }
    | {
          stepPosition: "last";
          currentStep: WizardStep<StepId>;
          cancel: () => void;
          prevStep: () => void;
      };

export type WizardBodyComponentProps = {
    title: React.ReactNode;
    description?: React.ReactNode;
    status?: "error" | "success" | "idle" | "pending";
    error?: Error | null;
};

export type WizardStep<StepId extends string> = {
    id: StepId;
    title: React.ReactNode;
    bodyComponent: (props: WizardBodyComponentProps) => JSX.Element;
    description?: React.ReactNode;
    status?: "error" | "success" | "idle" | "pending";
    error?: Error | null;
    skipValidation?: boolean;
    footerComponent?: (props: WizardFooterComponentProps<StepId>) => JSX.Element;
    beforeNext?: () => Promise<boolean>;
};

export type WizardSteps<StepId extends string> = Record<StepId, WizardStep<StepId>>;

type FormWizardProps<StepId extends string> = {
    steps: WizardSteps<StepId>;
    flow: StepId[];
    initialStep?: StepId;
    cancel: () => void;
};

export function FormWizard<StepId extends string>({ steps, flow, initialStep, cancel }: FormWizardProps<StepId>) {
    const [currentStepId, setCurrentStepId] = useState<StepId>(initialStep ?? flow[0]);
    const currentIndex = flow.indexOf(currentStepId);
    const currentStep = steps[currentStepId];

    const getStepPosition = (): WizardFooterComponentProps<StepId>["stepPosition"] => {
        if (currentIndex === 0) {
            return "first";
        }
        if (currentIndex === flow.length - 1) {
            return "last";
        }
        return "middle";
    };

    const footerProps: WizardFooterComponentProps<StepId> = {
        stepPosition: getStepPosition(),
        currentStep,
        nextStep: () => setCurrentStepId(flow[currentIndex + 1]),
        prevStep: () => setCurrentStepId(flow[currentIndex - 1]),
        cancel,
    };

    return (
        <div className="flex h-full min-h-full bg-background text-foreground">
            <div className="grid min-h-full w-full lg:grid-cols-[minmax(0,1fr)_13.75rem]">
                <div className="flex min-h-0 flex-col">
                    <div className="border-b px-5 py-3 lg:hidden">
                        <p className="mt-1 text-sm font-semibold">{currentStep.title}</p>
                    </div>

                    <main className="min-h-0 flex-1 overflow-auto px-5 py-12 sm:px-8 lg:px-24 lg:py-20">
                        <div className="mx-auto w-full max-w-5xl">
                            {
                                <currentStep.bodyComponent
                                    title={currentStep.title}
                                    description={currentStep.description}
                                    status={currentStep.status}
                                    error={currentStep.error}
                                />
                            }
                        </div>
                    </main>

                    {currentStep.footerComponent ? (
                        <currentStep.footerComponent {...footerProps} />
                    ) : (
                        <WizardFooter {...footerProps} />
                    )}
                </div>

                <aside className="hidden border-l bg-background px-4 py-5 lg:block" aria-label="Setup steps">
                    <WizardStepper flow={flow} steps={steps} currentIndex={currentIndex} />
                </aside>
            </div>
        </div>
    );
}

type WizardStepperProps<StepId extends string> = {
    flow: StepId[];
    currentIndex: number;
    steps: WizardSteps<StepId>;
};

function WizardStepper<StepId extends string>({ flow, currentIndex, steps }: WizardStepperProps<StepId>) {
    return (
        <ol className="space-y-4">
            {flow.map((stepId, index) => {
                const step = steps[stepId];
                const isCurrent = index === currentIndex;
                const isComplete = index < currentIndex;

                return (
                    <li key={stepId} className="flex items-center gap-3">
                        <StepIndicator isComplete={isComplete} isCurrent={isCurrent} />
                        <span
                            className={cn(
                                "text-sm leading-5 text-muted-foreground",
                                (isCurrent || isComplete) && "font-semibold text-foreground",
                            )}
                        >
                            {step.title}
                        </span>
                    </li>
                );
            })}
        </ol>
    );
}

function StepIndicator({ isComplete, isCurrent }: { isComplete: boolean; isCurrent: boolean }) {
    if (isComplete) {
        return (
            <span className="flex size-7 shrink-0 items-center justify-center rounded-full border border-foreground text-foreground">
                <Check className="size-3.5" aria-hidden="true" />
            </span>
        );
    }

    if (isCurrent) {
        return (
            <span className="flex size-7 shrink-0 items-center justify-center rounded-full border border-foreground text-foreground">
                <ArrowRight className="size-3.5" aria-hidden="true" />
            </span>
        );
    }

    return <span className="size-7 shrink-0 rounded-full bg-muted" />;
}

export function WizardFooter<T extends string>(props: WizardFooterComponentProps<T>) {
    return (
        <div className="flex border-t px-4 py-2">
            <WizardFooterBackButton {...props} />
            <WizardFooterNextButton {...props} />
        </div>
    );
}

export function WizardFooterBackButton<T extends string>(props: WizardFooterComponentProps<T>) {
    const { formState } = useFormContext();
    const isPending = props.currentStep.status === "pending" || formState.isSubmitting;

    return (
        <div className="flex gap-2">
            <Button onClick={props.cancel} variant="outline">
                Cancel
            </Button>
            {props.stepPosition !== "first" && (
                <Button onClick={props.prevStep} variant="secondary" disabled={isPending}>
                    Back
                </Button>
            )}
        </div>
    );
}

export function WizardFooterNextButton<T extends string>(props: WizardFooterComponentProps<T>) {
    const { trigger, formState } = useFormContext();

    const isPending = props.currentStep.status === "pending" || formState.isSubmitting;

    if (props.stepPosition === "last") {
        return (
            <Button type="submit" className="ml-auto" disabled={isPending}>
                {isPending && <Spinner />}
                Submit
            </Button>
        );
    }

    const handleNext = async () => {
        if (!props.currentStep.skipValidation) {
            const isValid = await trigger([props.currentStep.id]);
            if (!isValid) {
                return;
            }
        }

        if (props.currentStep.beforeNext) {
            const ok = await props.currentStep.beforeNext();
            if (!ok) {
                return;
            }
        }

        props.nextStep();
    };

    return (
        <Button onClick={handleNext} className="ml-auto" disabled={isPending}>
            {isPending && <Spinner />}
            Next
        </Button>
    );
}
