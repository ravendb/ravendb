import { useAppDispatch } from "components/store";
import { chatbotActions } from "../../store/chatbotSlice";
import "./ChatbotAskAiMessageFollowUpQuestions.scss";

interface ChatbotAskAiMessageFollowUpQuestionsProps {
    questions: string[];
}

export default function ChatbotAskAiMessageFollowUpQuestions({ questions }: ChatbotAskAiMessageFollowUpQuestionsProps) {
    const dispatch = useAppDispatch();

    if (!questions?.length) {
        return null;
    }

    return (
        <div className="pb-2 vstack gap-1">
            <span className="small-label">Follow up questions</span>
            <div className="vstack gap-1">
                {questions.filter(Boolean).map((question) => (
                    <div
                        key={question}
                        className="followup-question hover-filter"
                        onClick={() => dispatch(chatbotActions.runChat({ message: question }))}
                    >
                        {question}
                    </div>
                ))}
            </div>
        </div>
    );
}
