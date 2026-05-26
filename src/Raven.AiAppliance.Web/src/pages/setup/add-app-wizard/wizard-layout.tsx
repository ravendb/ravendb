import { ArrowLeft, ArrowRight, Check } from "lucide-react";
import type { ReactNode } from "react";
import { Link } from "react-router";
import { Button } from "@/components/shadcn/ui/button";
import { cn } from "@/lib/utils";
import {
    getStepIndex,
    getVisibleWizardSteps,
    SETUP_WIZARD_STEPS,
    type SetupWizardStepId,
} from "@/pages/setup/add-app-wizard/wizard-model";

type WizardLayoutProps = {
    canGoBack: boolean;
    children: ReactNode;
    currentStep: SetupWizardStepId;
    isWorking: boolean;
    nextLabel: string;
    onBack: () => void;
    onNext: () => void;
};

export function WizardLayout({
    canGoBack,
    children,
    currentStep,
    isWorking,
    nextLabel,
    onBack,
    onNext,
}: WizardLayoutProps) {
    return (
        <div className="flex min-h-full bg-background text-foreground">
            <div className="grid min-h-full w-full lg:grid-cols-[minmax(0,1fr)_13.75rem]">
                <form
                    className="flex min-h-0 flex-col"
                    onSubmit={(event) => {
                        event.preventDefault();
                        onNext();
                    }}
                >
                    <div className="border-b px-5 py-3 lg:hidden">
                        <p className="text-xs font-medium text-muted-foreground">
                            Step {getStepIndex(currentStep) + 1} of {SETUP_WIZARD_STEPS.length}
                        </p>
                        <p className="mt-1 text-sm font-semibold">{getStepLabel(currentStep)}</p>
                    </div>

                    <main className="min-h-0 flex-1 overflow-auto px-5 py-12 sm:px-8 lg:px-24 lg:py-20">
                        <div className="mx-auto w-full max-w-5xl">{children}</div>
                    </main>

                    <footer className="flex items-center justify-between gap-3 border-t px-5 py-4">
                        <div className="flex items-center gap-2">
                            <Button asChild variant="secondary">
                                <Link to="/">Cancel</Link>
                            </Button>
                            {canGoBack && (
                                <Button type="button" variant="secondary" onClick={onBack} disabled={isWorking}>
                                    <ArrowLeft className="size-4" aria-hidden="true" />
                                    Back
                                </Button>
                            )}
                        </div>
                        <Button type="submit" disabled={isWorking}>
                            {nextLabel}
                            <ArrowRight className="size-4" aria-hidden="true" />
                        </Button>
                    </footer>
                </form>

                <aside className="hidden border-l bg-background px-4 py-5 lg:block" aria-label="Setup steps">
                    <WizardStepper currentStep={currentStep} />
                </aside>
            </div>
        </div>
    );
}

function WizardStepper({ currentStep }: { currentStep: SetupWizardStepId }) {
    const currentIndex = getStepIndex(currentStep);
    const visibleSteps = getVisibleWizardSteps(currentStep);

    return (
        <ol className="space-y-4">
            {visibleSteps.map((step) => {
                const stepIndex = getStepIndex(step.id);
                const isCurrent = step.id === currentStep;
                const isComplete = stepIndex < currentIndex;

                return (
                    <li key={step.id} className="flex items-start gap-3">
                        <StepIndicator isComplete={isComplete} isCurrent={isCurrent} />
                        <span
                            className={cn(
                                "pt-1 text-sm leading-5 text-muted-foreground",
                                (isCurrent || isComplete) && "font-semibold text-foreground",
                            )}
                        >
                            {step.label}
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

function getStepLabel(stepId: SetupWizardStepId) {
    return SETUP_WIZARD_STEPS.find((step) => step.id === stepId)?.label ?? "";
}
