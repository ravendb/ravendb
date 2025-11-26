import { useAppDispatch } from "components/store";
import { chatbotActions } from "../../store/chatbotSlice";

interface ChatbotAskAiMessageFollowUpQuestionsProps {
    questions: string[];
}

export default function ChatbotAskAiMessageFollowUpQuestions({ questions }: ChatbotAskAiMessageFollowUpQuestionsProps) {
    const dispatch = useAppDispatch();

    if (!questions?.length) {
        return null;
    }

    return (
        <div className="mt-2">
            <span className="small-label">Follow up questions</span>
            <div className="vstack gap-1">
                {questions.filter(Boolean).map((question) => (
                    <div
                        key={question}
                        className="py-1 px-2 rounded-3 border border-primary cursor-pointer hover-filter"
                        onClick={() => dispatch(chatbotActions.runChat({ message: question }))}
                    >
                        {question}
                    </div>
                ))}
            </div>
        </div>
    );
}
