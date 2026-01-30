import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { chatbotActions, chatbotSelectors } from "./store/chatbotSlice";
import classNames from "classnames";
import "./ChatbotNavIcon.scss";

export default function ChatbotNavIcon() {
    const dispatch = useAppDispatch();
    const isOpen = useAppSelector(chatbotSelectors.isOpen);

    return (
        <Button
            title="Knowledge Center"
            variant="link"
            className={classNames("chatbot-nav-icon", { active: isOpen })}
            onClick={() => dispatch(chatbotActions.isOpenToggled())}
        >
            <Icon icon="ai-assistant" margin="m-0" className="icon-chatbot" />
        </Button>
    );
}
