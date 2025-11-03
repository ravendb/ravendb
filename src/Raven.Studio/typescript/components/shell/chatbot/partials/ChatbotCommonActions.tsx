import ClickableCard from "components/common/ClickableCard";
import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotActions, chatbotSelectors } from "../store/chatbotSlice";

export default function ChatbotCommonActions() {
    const dispatch = useAppDispatch();
    const messagesCount = useAppSelector(chatbotSelectors.messagesCount);

    if (messagesCount > 0) {
        return null;
    }

    return (
        <div className="p-2">
            <span className="small-label">Common actions</span>
            <div className="vstack gap-1">
                <ClickableCard
                    icon="document"
                    title="Explain this view"
                    description="Learn what this page does"
                    className="rounded-3"
                    onClick={() => dispatch(chatbotActions.runChat({ message: "Explain this view" }))}
                />
                <ClickableCard
                    icon="help"
                    title="Troubleshoot an error"
                    description="Find out what went wrong"
                    className="rounded-3"
                    onClick={() => dispatch(chatbotActions.runChat({ message: "Troubleshoot an error" }))}
                />
                <ClickableCard
                    icon="query"
                    title="Help with RQL"
                    description="See how to query data"
                    className="rounded-3"
                    onClick={() => dispatch(chatbotActions.runChat({ message: "Help with RQL" }))}
                />
                <ClickableCard
                    icon="rocket"
                    title="Suggest best practices"
                    description="Get tips to work smarter"
                    className="rounded-3"
                    onClick={() => dispatch(chatbotActions.runChat({ message: "Suggest best practices" }))}
                />
            </div>
        </div>
    );
}
