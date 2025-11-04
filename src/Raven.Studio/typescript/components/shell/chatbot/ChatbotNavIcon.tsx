import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { chatbotActions, chatbotSelectors } from "./store/chatbotSlice";
import classNames from "classnames";

export default function ChatbotNavIcon() {
    const dispatch = useAppDispatch();
    const isOpen = useAppSelector(chatbotSelectors.isOpen);

    return (
        <Button
            title="Chatbot"
            variant="link"
            className={classNames("p-0", { "text-reset": !isOpen })}
            onClick={() => dispatch(chatbotActions.isOpenToggled())}
        >
            <Icon icon="chatbot" margin="m-0" />
        </Button>
    );
}
