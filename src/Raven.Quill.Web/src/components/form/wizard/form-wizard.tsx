import { useEffect, useRef, useState, type ReactNode } from "react";
import {
    useFormContext,
    useFormState,
    useWatch,
    type Control,
    type FieldValues,
    type Path,
    type UseFormGetValues,
} from "react-hook-form";
import { ArrowLeft, ArrowRight, Check } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { useUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { Heading, Text } from "@/components/typography";
import { WizardErrorAlert } from "@/components/form/wizard/wizard-error-alert";
import { toError, WizardHandledError } from "@/components/form/wizard/wizard-step-error";
import { cn } from "@/lib/utils";

export type WizardProgress = {
    /**
     * Replaces the footer button label for the rest of the action, e.g. "Testing connection...".
     * Steps that run several calls in a row should report each one so the operator sees what the
     * wizard is waiting on instead of an unexplained spinner.
     */
    report: (label: string) => void;
};

export type WizardAction = (progress: WizardProgress) => void | Promise<void>;
export type WizardStepPosition = "first" | "middle" | "last";
export type WizardValidationTarget<Values extends FieldValues> = Path<Values> | readonly Path<Values>[] | false;

export type WizardCompletion =
    | { type: "submit"; label?: ReactNode; busyLabel?: ReactNode }
    | { type: "action"; label?: ReactNode; busyLabel?: ReactNode; onComplete: WizardAction };

export type WizardBodyComponentProps<StepId extends string = string> = {
    currentStepId: StepId;
    isBusy: boolean;
};

export type WizardFooterComponentProps = {
    isBusy: boolean;
};

export type WizardHeaderComponentProps = {
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
    isFullHeight?: boolean;
    validate: WizardValidationTarget<Values>;
    onValidationFailed?: WizardAction;
    beforeNext?: WizardAction;
    isNextDisabled?: boolean;
    nextLabel?: ReactNode;
    canCancel?: boolean;
    canGoBack?: boolean;
    footerComponent?: (props: WizardFooterComponentProps) => ReactNode;
    /** Rendered beside the step title, for actions that act on the whole configuration. */
    headerAction?: (props: WizardHeaderComponentProps) => ReactNode;
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
    /** The work is already persisted, so leaving loses nothing (for wizards that save before their last step). */
    isSaved?: boolean;
};

export function FormWizard<StepId extends string, Values extends FieldValues>({
    steps,
    flow,
    initialStep,
    cancel,
    completion = { type: "submit" },
    isSaved = false,
}: FormWizardProps<StepId, Values>) {
    const { trigger, control, getValues, subscribe } = useFormContext<Values>();
    // Not the context's formState proxy - it never re-renders this component, so these would go stale.
    const { isDirty, isSubmitting } = useFormState({ control });

    if (flow.length === 0) {
        throw new Error("FormWizard requires at least one step in the flow.");
    }

    const initialStepId = initialStep ?? flow[0];
    const [currentStepId, setCurrentStepId] = useState<StepId>(initialStepId);
    const [lastKnownIndex, setLastKnownIndex] = useState(() => Math.max(flow.indexOf(initialStepId), 0));
    const [isAdvancing, setIsAdvancing] = useState(false);
    const [progressLabel, setProgressLabel] = useState<string | null>(null);
    const [advanceError, setAdvanceError] = useState<Error | null>(null);
    const isAdvancingRef = useRef(false);

    // A failure describes the values it ran against, so any edit makes it stale. Steps that edit values
    // while advancing are unaffected: runAction sets the error after its action returned.
    useEffect(() => subscribe({ formState: { values: true }, callback: () => setAdvanceError(null) }), [subscribe]);

    const currentIndexInFlow = flow.indexOf(currentStepId);
    const currentIndex = currentIndexInFlow >= 0 ? currentIndexInFlow : Math.min(lastKnownIndex, flow.length - 1);

    const currentStepIdInFlow = flow[currentIndex];
    const currentStep = steps[currentStepIdInFlow];
    const stepPosition: WizardStepPosition =
        currentIndex === 0 ? "first" : currentIndex === flow.length - 1 ? "last" : "middle";
    const isBusy = isAdvancing || isSubmitting;

    // Every exit from a wizard is a route change, so registering is all the app-level guard needs.
    useUnsavedChanges(isDirty && !isSubmitting && !isSaved);

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

    const validateCurrentStep = async (progress: WizardProgress) => {
        if (currentStep.validate !== false) {
            // The trigger only works correctly when passing an array
            const isValid = await trigger(
                Array.isArray(currentStep.validate) ? currentStep.validate : [currentStep.validate],
            );

            if (!isValid) {
                await currentStep.onValidationFailed?.(progress);
                return false;
            }
        }

        return true;
    };

    const runAction = async (action: (progress: WizardProgress) => Promise<void>) => {
        // A ref closes the gap before React commits the disabled state, including the time spent
        // in async RHF validation.
        if (isAdvancingRef.current) {
            return;
        }

        isAdvancingRef.current = true;
        setIsAdvancing(true);
        setProgressLabel(null);
        setAdvanceError(null);

        try {
            await action({ report: setProgressLabel });
        } catch (error) {
            if (!(error instanceof WizardHandledError)) {
                setAdvanceError(toError(error));
            }
        } finally {
            isAdvancingRef.current = false;
            setIsAdvancing(false);
            setProgressLabel(null);
        }
    };

    const handleNext = async () => {
        if (currentIndex >= flow.length - 1) {
            return;
        }

        await runAction(async (progress) => {
            if (!(await validateCurrentStep(progress))) {
                return;
            }

            await currentStep.beforeNext?.(progress);
            setActiveStepIndex(currentIndex + 1);
        });
    };

    const handleComplete = async () => {
        if (completion.type !== "action") {
            return;
        }

        await runAction(async (progress) => {
            if (!(await validateCurrentStep(progress))) {
                return;
            }

            await currentStep.beforeNext?.(progress);
            await completion.onComplete(progress);
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
                                "mx-auto w-full max-w-7xl",
                                currentStep.isFullHeight ? "flex h-full flex-col gap-5" : "grid gap-5",
                            )}
                        >
                            <div className="flex flex-wrap items-start justify-between gap-3">
                                <div>
                                    <Heading as="h1" variant="page">
                                        {currentStep.title}
                                    </Heading>
                                    {currentStep.description && (
                                        <Text variant="muted" className="mt-3">
                                            {currentStep.description}
                                        </Text>
                                    )}
                                </div>
                                {currentStep.headerAction && <currentStep.headerAction isBusy={isBusy} />}
                            </div>

                            <currentStep.bodyComponent currentStepId={currentStepIdInFlow} isBusy={isBusy} />
                            {advanceError && <WizardErrorAlert error={advanceError} />}
                        </section>
                    </main>

                    <WizardFooter
                        stepPosition={stepPosition}
                        cancel={cancel}
                        goPrev={goPrev}
                        handleNext={handleNext}
                        handleComplete={handleComplete}
                        isBusy={isBusy}
                        progressLabel={progressLabel}
                        isNextDisabled={currentStep.isNextDisabled === true}
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

    return <div className="flex empty:hidden">{badge}</div>;
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
    progressLabel: string | null;
    isNextDisabled: boolean;
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
    progressLabel,
    isNextDisabled,
    nextLabel,
    canCancel,
    canGoBack,
    completion,
    footerComponent: FooterComponent,
}: WizardFooterProps<StepId>) {
    const isLast = stepPosition === "last";
    const isCompletionDisabled = isBusy || isNextDisabled;

    // A phase reported by the running step wins over the button's static busy label.
    const resolveLabel = (idleLabel: ReactNode, busyLabel?: ReactNode) =>
        progressLabel ?? (isBusy ? busyLabel : null) ?? idleLabel;

    return (
        <div className="border-t px-5 py-3 sm:px-8 lg:px-24">
            <div className={cn("mx-auto flex w-full max-w-7xl items-center")}>
                <div className="flex gap-2">
                    {canCancel && (
                        <Button onClick={cancel} variant="outline" size="lg" disabled={isBusy}>
                            Cancel
                        </Button>
                    )}
                    {stepPosition !== "first" && canGoBack && (
                        <Button onClick={goPrev} variant="secondary" size="lg" disabled={isBusy}>
                            <ArrowLeft aria-hidden="true" />
                            Back
                        </Button>
                    )}
                </div>
                <div className="ml-auto flex items-center gap-2" aria-live="polite">
                    {FooterComponent && <FooterComponent isBusy={isBusy} />}
                    {isLast && completion.type === "action" ? (
                        <Button
                            type="button"
                            onClick={handleComplete}
                            size="lg"
                            disabled={isCompletionDisabled}
                            key={`${currentStepId}:complete`}
                        >
                            {isBusy ? <Spinner /> : <Check aria-hidden="true" />}
                            <span>{resolveLabel(completion.label ?? "Finish", completion.busyLabel)}</span>
                        </Button>
                    ) : isLast ? (
                        <Button type="submit" size="lg" disabled={isCompletionDisabled} key={`${currentStepId}:submit`}>
                            {isBusy ? <Spinner /> : <Check aria-hidden="true" />}
                            <span>{resolveLabel(completion.label ?? "Submit", completion.busyLabel)}</span>
                        </Button>
                    ) : (
                        <Button
                            onClick={handleNext}
                            size="lg"
                            disabled={isCompletionDisabled}
                            key={`${currentStepId}:next`}
                        >
                            {isBusy && <Spinner />}
                            <span>{resolveLabel(nextLabel ?? "Next")}</span>
                            {!isBusy && <ArrowRight aria-hidden="true" />}
                        </Button>
                    )}
                </div>
            </div>
        </div>
    );
}
