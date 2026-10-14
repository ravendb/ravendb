import { useId, useState, type KeyboardEvent, type Ref } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, type FieldErrors } from "react-hook-form";
import { ArrowRight, ChevronLeft, ChevronRight, Sparkle } from "lucide-react";
import { FormRadioList } from "@/components/form/form-radio-list";
import { FormTextarea } from "@/components/form/form-textarea";
import { Button } from "@/components/shadcn/ui/button";
import { Heading, Text } from "@/components/typography";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import {
    answersFormSchema,
    CUSTOM_ANSWER,
    getInitialAnswers,
    recommendedAnswers,
    resolveAnswers,
    type AnswersFormData,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-questions-utils";
import { useCanAnswerPlanner } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlannerQuestion } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

/**
 * The planner's questions, one at a time, in place of the composer. The recommended answer is preselected,
 * number keys pick an option, and a question without options takes an answer in the operator's words.
 * Anything the operator wrote for the planner meanwhile goes along with the answers.
 */
export function PlannerQuestions({
    questions,
    className,
    ref,
}: {
    questions: PlannerQuestion[];
    className?: string;
    ref?: Ref<HTMLElement>;
}) {
    const session = usePlannerSession();
    const canAnswer = useCanAnswerPlanner();
    const titleId = useId();
    const draft = useSetupWizardStore((state) => state.plannerDraft.trim());
    const clearPlannerComposer = useSetupWizardStore((state) => state.clearPlannerComposer);
    const [index, setIndex] = useState(0);
    const form = useForm<AnswersFormData>({
        defaultValues: getInitialAnswers(questions),
        resolver: zodResolver(answersFormSchema),
        mode: "onChange",
    });

    const question = questions[index];
    const isLast = index === questions.length - 1;

    const showFirstUnanswered = (errors: FieldErrors<AnswersFormData>) => {
        const firstUnanswered = questions.findIndex((_, position) => errors.answers?.[position]);

        setIndex(Math.max(firstUnanswered, 0));
    };

    const send = form.handleSubmit(
        (values) => void session.answer(resolveAnswers(questions, values.answers), false),
        showFirstUnanswered,
    );

    const goToNextQuestion = async () => {
        // An array: given a single name, RHF re-renders only the field registered under exactly that name.
        if (await form.trigger([`answers.${index}`], { shouldFocus: true })) {
            setIndex(index + 1);
        }
    };

    const skip = () => void session.answer(recommendedAnswers(questions), true);

    const pick = (choice: string) => form.setValue(`answers.${index}.choice`, choice);

    const pickByNumber = (event: KeyboardEvent<HTMLElement>) => {
        const number = Number(event.key);
        const isTyping = event.target instanceof HTMLInputElement || event.target instanceof HTMLTextAreaElement;
        const hasModifier = event.ctrlKey || event.metaKey || event.altKey;

        if (isTyping || hasModifier || question.options.length === 0 || !Number.isInteger(number) || number < 1) {
            return;
        }

        if (number <= question.options.length) {
            event.preventDefault();
            pick(String(number - 1));
        } else if (number === question.options.length + 1) {
            event.preventDefault();
            pick(CUSTOM_ANSWER);
            form.setFocus(`answers.${index}.custom`);
        }
    };

    return (
        <section
            ref={ref}
            tabIndex={-1}
            aria-labelledby={titleId}
            className={cn(
                "mx-auto flex w-full flex-col gap-3 rounded-xl border bg-card px-4 pt-3.5 pb-3 shadow-floating outline-none",
                className,
            )}
            onKeyDown={pickByNumber}
        >
            <div className="flex items-center gap-2">
                <Sparkle className="size-3 shrink-0 text-primary-strong" aria-hidden="true" />
                <Text variant="muted" as="span">
                    {questions.length === 1
                        ? "The planner has a question"
                        : `The planner has ${questions.length} questions`}
                </Text>
                <Text variant="muted" as="span" className="ml-auto">
                    {index + 1} of {questions.length}
                </Text>
                <div className="flex gap-0.5">
                    <Button
                        variant="ghost"
                        size="icon-sm"
                        className="text-muted-foreground"
                        aria-label="Previous question"
                        disabled={index === 0}
                        onClick={() => setIndex(index - 1)}
                    >
                        <ChevronLeft aria-hidden="true" />
                    </Button>
                    <Button
                        variant="ghost"
                        size="icon-sm"
                        className="text-muted-foreground"
                        aria-label="Next question"
                        disabled={isLast}
                        onClick={() => setIndex(index + 1)}
                    >
                        <ChevronRight aria-hidden="true" />
                    </Button>
                </div>
            </div>

            <Heading as="h3" variant="subsection" id={titleId}>
                {question.question}
            </Heading>

            {question.options.length > 0 ? (
                <FormRadioList
                    key={index}
                    control={form.control}
                    name={`answers.${index}.choice`}
                    aria-labelledby={titleId}
                    options={question.options.map((option, optionIndex) => ({
                        value: String(optionIndex),
                        label: optionIndex === question.recommended ? `${option} (Recommended)` : option,
                        hint: <OptionKey>{optionIndex + 1}</OptionKey>,
                    }))}
                    other={{
                        name: `answers.${index}.custom`,
                        value: CUSTOM_ANSWER,
                        label: "Something else",
                        placeholder: "Something else…",
                        hint: <OptionKey>{question.options.length + 1}</OptionKey>,
                    }}
                />
            ) : (
                <FormTextarea
                    key={index}
                    control={form.control}
                    name={`answers.${index}.custom`}
                    aria-labelledby={titleId}
                    placeholder="Your answer"
                    rows={2}
                />
            )}

            {draft && (
                <Text variant="muted">
                    Your note goes with the answers: &ldquo;{draft}&rdquo;{" "}
                    <Button variant="link" className="h-auto p-0" onClick={clearPlannerComposer}>
                        Discard it
                    </Button>
                </Text>
            )}

            <div className="flex flex-wrap items-center gap-2">
                <Button variant="ghost" className="text-muted-foreground" disabled={!canAnswer} onClick={skip}>
                    Skip, use recommended answers
                </Button>
                <Button className="ml-auto" disabled={isLast && !canAnswer} onClick={isLast ? send : goToNextQuestion}>
                    {isLast ? "Send answers" : "Next"}
                    <ArrowRight data-icon="inline-end" />
                </Button>
            </div>
        </section>
    );
}

function OptionKey({ children }: { children: number }) {
    return (
        <kbd
            aria-hidden="true"
            className="inline-flex h-5.5 min-w-5.5 items-center justify-center rounded-sm border bg-muted px-1.5 text-muted-foreground"
        >
            <Text as="span" variant="code">
                {children}
            </Text>
        </kbd>
    );
}
