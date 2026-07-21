import { useRef, useState, type ReactNode } from "react";
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

export type WizardAction = () => void | Promise<void>;
export type WizardStepPosition = "first" | "middle" | "last";
export type WizardValidationTarget<Values extends FieldValues> = Path<Values> | readonly Path<Values>[] | false;

export type WizardCompletion =
    | { type: "submit"; label?: ReactNode }
    | { type: "action"; label?: ReactNode; onComplete: WizardAction };

export type WizardBodyComponentProps<StepId extends string = string> = {
    currentStepId: StepId;
    isBusy: boolean;
};

export type WizardFooterComponentProps = {
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
          badgeFields?: readonly Path<Values>[];
      };

export type WizardStep<StepId extends string, Values extends FieldValues = FieldValues> = {
    title: ReactNode;
    description?: ReactNode;
    bodyComponent: (props: WizardBodyComponentProps<StepId>) => ReactNode;
    /** Makes the step body fill the visible area so it can manage its own scrolling. */
    isFullHeight?: boolean;
    validate: WizardValidationTarget<Values>;
    beforeNext?: WizardAction;
    nextLabel?: ReactNode;
    canCancel?: boolean;
    canGoBack?: boolean;
    footerComponent?: (props: WizardFooterComponentProps) => ReactNode;
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
    completion?: WizardCompletion;
};

export function FormWizard<StepId extends string, Values extends FieldValues>({
    steps,
    flow,
    initialStep,
    cancel,
    completion = { type: "submit" },
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
    const isAdvancingRef = useRef(false);

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

    const validateCurrentStep = async () => {
        if (currentStep.validate !== false) {
            // The trigger only works correctly when passing an array
            const isValid = await trigger(
                Array.isArray(currentStep.validate) ? currentStep.validate : [currentStep.validate],
            );

            if (!isValid) {
                return false;
            }
        }

        return true;
    };

    const runAction = async (action: () => Promise<void>) => {
        // A ref closes the gap before React commits the disabled state, including the time spent
        // in async RHF validation.
        if (isAdvancingRef.current) {
            return;
        }

        isAdvancingRef.current = true;
        setIsAdvancing(true);
        setAdvanceError(null);

        try {
            await action();
        } catch (error) {
            setAdvanceError(error instanceof Error ? error : new Error(String(error)));
        } finally {
            isAdvancingRef.current = false;
            setIsAdvancing(false);
        }
    };

    const handleNext = async () => {
        if (currentIndex >= flow.length - 1) {
            return;
        }

        await runAction(async () => {
            if (!(await validateCurrentStep())) {
                return;
            }

            await currentStep.beforeNext?.();
            setActiveStepIndex(currentIndex + 1);
        });
    };

    const handleComplete = async () => {
        if (completion.type !== "action") {
            return;
        }

        await runAction(async () => {
            if (!(await validateCurrentStep())) {
                return;
            }

            await currentStep.beforeNext?.();
            await completion.onComplete();
        });
    };

    return (
        <div className="flex h-full min-h-full text-foreground">
            {/* grid-cols-1 matters: an implicit auto column would size to its content's max-content
                width, letting a wide table prop the layout open instead of shrinking with the window. */}
            <div className="grid min-h-full w-full grid-cols-1 lg:grid-cols-[minmax(0,1fr)_13.75rem]">
                <div className="flex min-h-0 flex-col">
                    <div className="border-b px-5 py-3 lg:hidden">
                        <p className="mt-1 text-sm font-semibold">{currentStep.title}</p>
                    </div>

                    <main className="min-h-0 flex-1 overflow-auto px-5 py-12 sm:px-8 lg:px-24 lg:py-20">
                        <section
                            key={currentStepIdInFlow}
                            className={cn(
                                "mx-auto w-full max-w-5xl",
                                currentStep.isFullHeight ? "flex h-full flex-col gap-5" : "grid gap-5",
                            )}
                        >
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
                        handleComplete={handleComplete}
                        isBusy={isBusy}
                        currentStepId={currentStepIdInFlow}
                        nextLabel={currentStep.nextLabel}
                        canCancel={currentStep.canCancel !== false}
                        canGoBack={currentStep.canGoBack !== false}
                        completion={completion}
                        footerComponent={currentStep.footerComponent}
                    />
                </div>

                <aside className="hidden border-l bg-sidebar/30 px-4 py-5 lg:block" aria-label="Setup steps">
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
                    <li key={stepId} className="flex items-start gap-3" aria-current={isCurrent ? "step" : undefined}>
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
        badgeFields?: readonly Path<Values>[];
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
    // An empty name array subscribes to nothing, so steps with a static badge can omit badgeFields.
    useWatch({ control, name: step.badgeFields ?? [] });
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
    handleComplete: () => Promise<void>;
    isBusy: boolean;
    nextLabel?: ReactNode;
    canCancel: boolean;
    canGoBack: boolean;
    completion: WizardCompletion;
    footerComponent?: (props: WizardFooterComponentProps) => ReactNode;
};

function WizardFooter<StepId extends string>({
    stepPosition,
    currentStepId,
    cancel,
    goPrev,
    handleNext,
    handleComplete,
    isBusy,
    nextLabel,
    canCancel,
    canGoBack,
    completion,
    footerComponent: FooterComponent,
}: WizardFooterProps<StepId>) {
    const isLast = stepPosition === "last";

    return (
        <div className="flex items-center border-t px-4 py-2">
            <div className="flex gap-2">
                {canCancel && (
                    <Button onClick={cancel} variant="outline" disabled={isBusy}>
                        Cancel
                    </Button>
                )}
                {stepPosition !== "first" && canGoBack && (
                    <Button onClick={goPrev} variant="secondary" disabled={isBusy}>
                        Back
                    </Button>
                )}
            </div>

            <div className="ml-auto flex items-center gap-2">
                {FooterComponent && <FooterComponent isBusy={isBusy} />}
                {isLast && completion.type === "action" ? (
                    <Button type="button" onClick={handleComplete} disabled={isBusy} key={`${currentStepId}:complete`}>
                        {isBusy && <Spinner />}
                        {completion.label ?? "Finish"}
                    </Button>
                ) : isLast ? (
                    <Button type="submit" disabled={isBusy} key={`${currentStepId}:submit`}>
                        {isBusy && <Spinner />}
                        {completion.label ?? "Submit"}
                    </Button>
                ) : (
                    <Button onClick={handleNext} disabled={isBusy} key={`${currentStepId}:next`}>
                        {isBusy && <Spinner />}
                        {nextLabel ?? "Next"}
                    </Button>
                )}
            </div>
        </div>
    );
}
