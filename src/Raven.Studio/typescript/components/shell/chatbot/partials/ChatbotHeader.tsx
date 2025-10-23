import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";

export default function ChatbotHeader() {
    const dispatch = useAppDispatch();

    const isPinned = useAppSelector(chatbotSelectors.isPinned);

    return (
        <div
            className={classNames(
                "panel-bg-2 border-bottom border-secondary p-2 hstack justify-content-between align-items-center",
                {
                    "rounded-top-2": !isPinned,
                }
            )}
        >
            <h4 className="m-0">
                <HeaderTitle />
            </h4>
            <div className="hstack">
                <Button
                    variant="link"
                    size="sm"
                    className="text-reset"
                    onClick={() => dispatch(chatbotActions.messagesSet([]))}
                >
                    <Icon icon="plus" margin="m-0" />
                </Button>
                {isPinned ? (
                    <Button
                        variant="link"
                        size="sm"
                        className="text-reset"
                        onClick={() => dispatch(chatbotActions.isPinnedSet(false))}
                    >
                        <Icon icon="pinned" margin="m-0" />
                    </Button>
                ) : (
                    <Button
                        variant="link"
                        size="sm"
                        className="text-reset"
                        onClick={() => dispatch(chatbotActions.isPinnedSet(true))}
                    >
                        <Icon icon="pin" margin="m-0" />
                    </Button>
                )}
                <Button
                    variant="link"
                    size="sm"
                    onClick={() => dispatch(chatbotActions.isOpenSet(false))}
                    className="text-reset"
                >
                    <Icon icon="cancel" margin="m-0" />
                </Button>
            </div>
        </div>
    );
}

function HeaderTitle() {
    const dispatch = useAppDispatch();

    const chatbotTab = useAppSelector(chatbotSelectors.chatbotTab);
    const resourcesTab = useAppSelector(chatbotSelectors.chatbotResourcesTab);

    const handleResourcesGoBack = () => {
        dispatch(chatbotActions.chatbotResourcesTabSet("Help and resources"));
    };

    if (chatbotTab === "askAi") {
        return (
            <div>
                <Icon icon="ai" />
                Ask AI
            </div>
        );
    }

    if (chatbotTab === "resources") {
        if (resourcesTab === "Help and resources") {
            return (
                <div>
                    <Icon icon="resources" />
                    Resources
                </div>
            );
        }

        return (
            <div>
                <Button variant="link" size="sm" className="text-reset p-0 pe-1" onClick={handleResourcesGoBack}>
                    <Icon icon="arrow-thin-left" margin="m-0" />
                </Button>
                {resourcesTab}
            </div>
        );
    }

    return null;
}
