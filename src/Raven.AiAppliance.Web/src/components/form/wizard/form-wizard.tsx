import { useState, type ReactNode } from "react";
import {
    useFormContext,
    useWatch,
    type Control,
    type FieldValues,
    type Path,
    type UseFormGetValues,
} from "react-hook-form";
import { ArrowRight, Check } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Alert } from "@/components/shadcn/ui/alert";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { cn } from "@/lib/utils";

export type WizardBeforeNext = () => void | Promise<void>;
export type WizardStepPosition = "first" | "middle" | "last";
export type WizardValidationTarget<Values extends FieldValues> = Path<Values> | readonly Path<Values>[] | false;

export type WizardBodyComponentProps<StepId extends string = string> = {
    currentStepId: StepId;
    isBusy: boolean;
};

export type WizardBadgeContext<Values extends FieldValues = FieldValues> = {
    values: Values;
    isComplete: boolean;
    isCurrent: boolean;
};

type WizardStepBadge<Values extends FieldValues> =
    | {
          badge?: undefined;
          badgeFields?: undefined;
      }
    | {
          badge: (context: WizardBadgeContext<Values>) => ReactNode;
          badgeFields: readonly Path<Values>[];
      };

export type WizardStep<StepId extends string, Values extends FieldValues = FieldValues> = {
    title: ReactNode;
    description?: ReactNode;
    bodyComponent: (props: WizardBodyComponentProps<StepId>) => ReactNode;
    validate: WizardValidationTarget<Values>;
    beforeNext?: WizardBeforeNext;
} & WizardStepBadge<Values>;

export type WizardSteps<StepId extends string, Values extends FieldValues = FieldValues> = Record<
    StepId,
    WizardStep<StepId, Values>
>;

type FormWizardProps<StepId extends string, Values extends FieldValues> = {
    steps: WizardSteps<StepId, Values>;
    flow: StepId[];
    initialStep?: StepId;
    cancel: () => void;
    submitLabel?: ReactNode;
};

export function FormWizard<StepId extends string, Values extends FieldValues>({
    steps,
    flow,
    initialStep,
    submitLabel,
    cancel,
}: FormWizardProps<StepId, Values>) {
    const { trigger, control, getValues, formState } = useFormContext<Values>();

    if (flow.length === 0) {
        throw new Error("FormWizard requires at least one step in the flow.");
    }

    const initialStepId = initialStep ?? flow[0];
    const [currentStepId, setCurrentStepId] = useState<StepId>(initialStepId);
    const [lastKnownIndex, setLastKnownIndex] = useState(() => Math.max(flow.indexOf(initialStepId), 0));
    const [isAdvancing, setIsAdvancing] = useState(false);
    const [advanceError, setAdvanceError] = useState<Error | null>(null);

    const currentIndexInFlow = flow.indexOf(currentStepId);
    const currentIndex = currentIndexInFlow >= 0 ? currentIndexInFlow : Math.min(lastKnownIndex, flow.length - 1);

    const currentStepIdInFlow = flow[currentIndex];
    const currentStep = steps[currentStepIdInFlow];
    const stepPosition: WizardStepPosition =
        currentIndex === 0 ? "first" : currentIndex === flow.length - 1 ? "last" : "middle";
    const isBusy = isAdvancing || formState.isSubmitting;

    const setActiveStepIndex = (index: number) => {
        setLastKnownIndex(index);
        setCurrentStepId(flow[index]);
    };

    const goPrev = () => {
        if (currentIndex === 0) {
            return;
        }

        setAdvanceError(null);
        setActiveStepIndex(currentIndex - 1);
    };

    const handleNext = async () => {
        if (currentIndex >= flow.length - 1) {
            return;
        }

        if (currentStep.validate !== false) {
            // The trigger only works correctly when passing an array
            const isValid = await trigger(
                Array.isArray(currentStep.validate) ? currentStep.validate : [currentStep.validate],
            );

            if (!isValid) {
                return;
            }
        }

        setIsAdvancing(true);
        setAdvanceError(null);

        try {
            if (currentStep.beforeNext) {
                await currentStep.beforeNext();
            }

            setActiveStepIndex(currentIndex + 1);
        } catch (error) {
            setAdvanceError(error instanceof Error ? error : new Error(String(error)));
        } finally {
            setIsAdvancing(false);
        }
    };

    return (
        <div className="flex h-full min-h-full bg-background text-foreground">
            <div className="grid min-h-full w-full lg:grid-cols-[minmax(0,1fr)_13.75rem]">
                <div className="flex min-h-0 flex-col">
                    <div className="border-b px-5 py-3 lg:hidden">
                        <p className="mt-1 text-sm font-semibold">{currentStep.title}</p>
                    </div>

                    <main className="min-h-0 flex-1 overflow-auto px-5 py-12 sm:px-8 lg:px-24 lg:py-20">
                        <section key={currentStepIdInFlow} className="mx-auto grid w-full max-w-5xl gap-5">
                            <div>
                                <h2 className="text-2xl font-semibold tracking-normal">{currentStep.title}</h2>
                                {currentStep.description && (
                                    <p className="mt-3 text-sm text-muted-foreground">{currentStep.description}</p>
                                )}
                            </div>

                            <currentStep.bodyComponent currentStepId={currentStepIdInFlow} isBusy={isBusy} />
                            {advanceError && <Alert variant="destructive">{advanceError.message}</Alert>}
                        </section>
                    </main>

                    <WizardFooter
                        stepPosition={stepPosition}
                        cancel={cancel}
                        goPrev={goPrev}
                        handleNext={handleNext}
                        isBusy={isBusy}
                        currentStepId={currentStepIdInFlow}
                        submitLabel={submitLabel}
                    />
                </div>

                <aside className="hidden border-l bg-background px-4 py-5 lg:block" aria-label="Setup steps">
                    <WizardStepper
                        flow={flow}
                        steps={steps}
                        currentIndex={currentIndex}
                        control={control}
                        getValues={getValues}
                    />
                </aside>
            </div>
        </div>
    );
}

type WizardStepperProps<StepId extends string, Values extends FieldValues> = {
    flow: StepId[];
    currentIndex: number;
    steps: WizardSteps<StepId, Values>;
    control: Control<Values>;
    getValues: UseFormGetValues<Values>;
};

function WizardStepper<StepId extends string, Values extends FieldValues>({
    flow,
    currentIndex,
    steps,
    control,
    getValues,
}: WizardStepperProps<StepId, Values>) {
    return (
        <ol className="space-y-4">
            {flow.map((stepId, index) => {
                const step = steps[stepId];
                const isCurrent = index === currentIndex;
                const isComplete = index < currentIndex;

                return (
                    <li key={stepId} className="flex items-start gap-3">
                        <StepIndicator isComplete={isComplete} isCurrent={isCurrent} />
                        <div className="grid gap-1.5">
                            <span
                                className={cn(
                                    "text-sm leading-5 text-muted-foreground",
                                    (isCurrent || isComplete) && "font-semibold text-foreground",
                                )}
                            >
                                {step.title}
                            </span>
                            {step.badge && (
                                <WizardStepBadge
                                    step={step}
                                    isComplete={isComplete}
                                    isCurrent={isCurrent}
                                    control={control}
                                    getValues={getValues}
                                />
                            )}
                        </div>
                    </li>
                );
            })}
        </ol>
    );
}

type WizardStepBadgeProps<StepId extends string, Values extends FieldValues> = {
    step: WizardStep<StepId, Values> & {
        badge: (context: WizardBadgeContext<Values>) => ReactNode;
        badgeFields: readonly Path<Values>[];
    };
    isComplete: boolean;
    isCurrent: boolean;
    control: Control<Values>;
    getValues: UseFormGetValues<Values>;
};

function WizardStepBadge<StepId extends string, Values extends FieldValues>({
    step,
    isComplete,
    isCurrent,
    control,
    getValues,
}: WizardStepBadgeProps<StepId, Values>) {
    useWatch({ control, name: step.badgeFields });
    const badge = step.badge({ values: getValues(), isComplete, isCurrent });

    if (!badge) {
        return null;
    }

    return <div className="flex">{badge}</div>;
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

type WizardFooterProps<StepId extends string> = {
    stepPosition: WizardStepPosition;
    currentStepId: StepId;
    cancel: () => void;
    goPrev: () => void;
    handleNext: () => Promise<void>;
    isBusy: boolean;
    submitLabel?: ReactNode;
};

function WizardFooter<StepId extends string>({
    stepPosition,
    currentStepId,
    cancel,
    goPrev,
    handleNext,
    isBusy,
    submitLabel,
}: WizardFooterProps<StepId>) {
    return (
        <div className="flex border-t px-4 py-2">
            <div className="flex gap-2">
                <Button onClick={cancel} variant="outline">
                    Cancel
                </Button>
                {stepPosition !== "first" && (
                    <Button onClick={goPrev} variant="secondary" disabled={isBusy}>
                        Back
                    </Button>
                )}
            </div>

            {stepPosition === "last" ? (
                <Button type="submit" className="ml-auto" disabled={isBusy} key={`${currentStepId}:submit`}>
                    {isBusy && <Spinner />}
                    {submitLabel ?? "Submit"}
                </Button>
            ) : (
                <Button onClick={handleNext} className="ml-auto" disabled={isBusy} key={`${currentStepId}:next`}>
                    {isBusy && <Spinner />}
                    Next
                </Button>
            )}
        </div>
    );
}
