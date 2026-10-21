import { useEffect, useId, useRef, useState, type ReactNode } from "react";
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
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { Progress } from "@/components/shadcn/ui/progress";
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

export type WizardConfirmNext = {
    isRequired: boolean;
    title: ReactNode;
    description?: ReactNode;
    confirmLabel?: ReactNode;
    /** Runs only once Next passed its checks and `beforeNext`, right before the wizard moves on. */
    onConfirm?: () => void;
};

/** Lets a step move the wizard itself, for actions inside the step that end in another step. */
export type WizardStepNavigation = {
    goToPreviousStep: () => void;
    /**
     * Moves on once `action` succeeds, for a step that can finish another way than its Next. The action
     * runs like Next does - busy, guarded against a second click, its failure shown - but in place of the
     * step's own checks and `beforeNext`.
     */
    advanceWith: (action: () => void | Promise<void>) => void;
};

export type WizardBodyComponentProps<StepId extends string = string> = WizardStepNavigation & {
    currentStepId: StepId;
    isBusy: boolean;
};

export type WizardFooterComponentProps = {
    isBusy: boolean;
};

export type WizardHeaderComponentProps = WizardStepNavigation & {
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
    confirmNext?: WizardConfirmNext;
    isNextDisabled?: boolean;
    /** Shown beside the Next button and announced with it: what Next will do, or why it is held back. */
    nextNote?: ReactNode;
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
    const [isConfirmingNext, setIsConfirmingNext] = useState(false);
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
        if (currentIndex === 0 || isAdvancingRef.current) {
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

    const advanceWith = (action: () => void | Promise<void>) => {
        if (currentIndex >= flow.length - 1) {
            return;
        }

        void runAction(async () => {
            await action();
            setActiveStepIndex(currentIndex + 1);
        });
    };

    const advance = async (onPassed?: () => void) => {
        await runAction(async (progress) => {
            if (!(await validateCurrentStep(progress))) {
                return;
            }

            await currentStep.beforeNext?.(progress);
            onPassed?.();
            setActiveStepIndex(currentIndex + 1);
        });
    };

    const handleNext = async () => {
        if (currentIndex >= flow.length - 1) {
            return;
        }

        if (currentStep.confirmNext?.isRequired) {
            setIsConfirmingNext(true);
            return;
        }

        await advance();
    };

    const confirmNext = () => void advance(currentStep.confirmNext?.onConfirm);

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
            <div className="grid min-h-full w-full grid-cols-1 2xl:grid-cols-[minmax(0,1fr)_13.75rem]">
                <div className="flex min-h-0 flex-col">
                    <main className="min-h-0 flex-1 overflow-auto p-6 2xl:p-12">
                        <section
                            key={currentStepIdInFlow}
                            className={cn(
                                "mx-auto w-full max-w-7xl",
                                currentStep.isFullHeight
                                    ? "flex h-full flex-col gap-4 2xl:gap-5"
                                    : "grid gap-4 2xl:gap-5",
                            )}
                        >
                            <header className="flex flex-wrap items-start justify-between gap-3">
                                <div>
                                    <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                                        <Heading as="h1" variant="page">
                                            {currentStep.title}
                                        </Heading>
                                        <WizardStepCount currentIndex={currentIndex} stepCount={flow.length} />
                                    </div>
                                    {currentStep.description && (
                                        <Text variant="muted" className="mt-3">
                                            {currentStep.description}
                                        </Text>
                                    )}
                                </div>
                                {currentStep.headerAction && (
                                    <currentStep.headerAction
                                        isBusy={isBusy}
                                        goToPreviousStep={goPrev}
                                        advanceWith={advanceWith}
                                    />
                                )}
                            </header>

                            <currentStep.bodyComponent
                                currentStepId={currentStepIdInFlow}
                                isBusy={isBusy}
                                goToPreviousStep={goPrev}
                                advanceWith={advanceWith}
                            />
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
                        nextNote={currentStep.nextNote}
                        currentStepId={currentStepIdInFlow}
                        nextLabel={currentStep.nextLabel}
                        canCancel={currentStep.canCancel !== false}
                        canGoBack={currentStep.canGoBack !== false}
                        completion={completion}
                        footerComponent={currentStep.footerComponent}
                    />
                    {currentStep.confirmNext && (
                        <ConfirmDialog
                            variant="warning"
                            open={isConfirmingNext}
                            onOpenChange={setIsConfirmingNext}
                            title={currentStep.confirmNext.title}
                            description={currentStep.confirmNext.description}
                            confirmLabel={currentStep.confirmNext.confirmLabel}
                            onConfirm={confirmNext}
                        />
                    )}
                </div>

                <aside className="hidden border-l bg-sidebar/30 px-4 py-5 2xl:block" aria-label="Setup steps">
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

/** Stands in for the stepper on screens too narrow to show it. */
function WizardStepCount({ currentIndex, stepCount }: { currentIndex: number; stepCount: number }) {
    const stepNumber = currentIndex + 1;

    return (
        <Badge variant="outline" className="h-7 gap-3 px-3 2xl:hidden">
            <Progress value={(stepNumber / stepCount) * 100} className="w-14" aria-hidden="true" />
            Step {stepNumber}/{stepCount}
        </Badge>
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
    nextNote?: ReactNode;
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
    nextNote,
    nextLabel,
    canCancel,
    canGoBack,
    completion,
    footerComponent: FooterComponent,
}: WizardFooterProps<StepId>) {
    const nextNoteId = useId();
    const isLast = stepPosition === "last";
    const isCompletionDisabled = isBusy || isNextDisabled;

    // A phase reported by the running step wins over the button's static busy label.
    const resolveLabel = (idleLabel: ReactNode, busyLabel?: ReactNode) =>
        progressLabel ?? (isBusy ? busyLabel : null) ?? idleLabel;

    return (
        <div className="border-t px-6 py-3 2xl:px-12">
            <div className="mx-auto flex w-full max-w-7xl flex-wrap items-center gap-x-4 gap-y-2">
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
                <div className="ml-auto flex flex-wrap items-center justify-end gap-x-4 gap-y-2" aria-live="polite">
                    {nextNote && (
                        <Text variant="muted" as="span" id={nextNoteId}>
                            {nextNote}
                        </Text>
                    )}
                    <div className="flex items-center gap-2">
                        {FooterComponent && <FooterComponent isBusy={isBusy} />}
                        {isLast && completion.type === "action" ? (
                            <Button
                                type="button"
                                onClick={handleComplete}
                                size="lg"
                                disabled={isCompletionDisabled}
                                key={`${currentStepId}:complete`}
                                aria-describedby={nextNote ? nextNoteId : undefined}
                            >
                                {isBusy ? <Spinner /> : <Check aria-hidden="true" />}
                                <span>{resolveLabel(completion.label ?? "Finish", completion.busyLabel)}</span>
                            </Button>
                        ) : isLast ? (
                            <Button
                                type="submit"
                                size="lg"
                                disabled={isCompletionDisabled}
                                key={`${currentStepId}:submit`}
                                aria-describedby={nextNote ? nextNoteId : undefined}
                            >
                                {isBusy ? <Spinner /> : <Check aria-hidden="true" />}
                                <span>{resolveLabel(completion.label ?? "Submit", completion.busyLabel)}</span>
                            </Button>
                        ) : (
                            <Button
                                onClick={handleNext}
                                size="lg"
                                disabled={isCompletionDisabled}
                                key={`${currentStepId}:next`}
                                aria-describedby={nextNote ? nextNoteId : undefined}
                            >
                                {isBusy && <Spinner />}
                                <span>{resolveLabel(nextLabel ?? "Next")}</span>
                                {!isBusy && <ArrowRight aria-hidden="true" />}
                            </Button>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
