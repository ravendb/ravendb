import { useId, useState } from "react";
import { X } from "lucide-react";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Label } from "@/components/shadcn/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/shadcn/ui/radio-group";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { Heading, Text } from "@/components/typography";
import {
    areAllQuestionsAnswered,
    composeAnswersPrompt,
    recommendedAnswers,
    type PlannerQuestion,
} from "@/pages/setup/add-app-wizard/steps/map/planner-questions-utils";

/**
 * Takes the composer's place while the planner is waiting on a decision. Every question needs a
 * pick before the answers go out; closing the panel is the one way around that, and it answers
 * with the planner's own recommendations.
 */
export function PlannerQuestions({
    questions,
    onAnswer,
}: {
    questions: PlannerQuestion[];
    onAnswer: (prompt: string) => void;
}) {
    const [answers, setAnswers] = useState<(number | undefined)[]>([]);
    const isComplete = areAllQuestionsAnswered(questions, answers);

    return (
        <div className="grid max-h-[60%] min-h-0 grid-rows-[auto_1fr_auto] border-t">
            <div className="flex items-center gap-2 px-3 pt-3">
                <Heading as="h3" variant="label" className="flex-1">
                    {questions.length === 1
                        ? "The planner has a question"
                        : `The planner has ${questions.length} questions`}
                </Heading>
                <TooltipProvider>
                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button
                                type="button"
                                size="icon-sm"
                                variant="ghost"
                                aria-label="Skip the questions and use the recommended answers"
                                onClick={() =>
                                    onAnswer(composeAnswersPrompt(questions, recommendedAnswers(questions), true))
                                }
                            >
                                <X aria-hidden />
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>Skip - use the recommended answers</TooltipContent>
                    </Tooltip>
                </TooltipProvider>
            </div>

            <ol className="grid min-h-0 gap-4 overflow-y-auto p-3">
                {questions.map((question, index) => (
                    <PlannerQuestionItem
                        key={`${index}-${question.question}`}
                        index={index}
                        question={question}
                        answer={answers[index]}
                        onAnswerChange={(answer) =>
                            setAnswers((current) => {
                                const next = [...current];
                                next[index] = answer;
                                return next;
                            })
                        }
                    />
                ))}
            </ol>

            <div className="flex justify-end px-3 pb-3">
                <Button
                    type="button"
                    disabled={!isComplete}
                    onClick={() => onAnswer(composeAnswersPrompt(questions, answers, false))}
                >
                    Submit answers
                </Button>
            </div>
        </div>
    );
}

function PlannerQuestionItem({
    index,
    question,
    answer,
    onAnswerChange,
}: {
    index: number;
    question: PlannerQuestion;
    answer: number | undefined;
    onAnswerChange: (answer: number) => void;
}) {
    const id = useId();

    return (
        <li className="grid gap-2">
            <Text variant="label" as="p" id={`${id}-question`}>
                {index + 1}. {question.question}
            </Text>
            <RadioGroup
                aria-labelledby={`${id}-question`}
                value={answer === undefined ? "" : String(answer)}
                onValueChange={(value) => onAnswerChange(Number(value))}
                className="pl-4"
            >
                {question.options.map((option, optionIndex) => (
                    <div key={option} className="flex items-center gap-2">
                        <RadioGroupItem value={String(optionIndex)} id={`${id}-${optionIndex}`} />
                        <Label htmlFor={`${id}-${optionIndex}`} className="font-normal">
                            {option}
                        </Label>
                        {optionIndex === question.recommended && <Badge variant="info">Recommended</Badge>}
                    </div>
                ))}
            </RadioGroup>
        </li>
    );
}
