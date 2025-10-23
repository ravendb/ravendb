import "./Chatbot.scss";
import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotActions, chatbotSelectors } from "./store/chatbotSlice";
import useResizableWidth from "components/hooks/useResizableWidth";
import ColumnResize from "components/common/ColumnResize";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import IconName from "typings/server/icons";
import classNames from "classnames";
import ChatbotMessages from "./partials/ChatbotMessages";
import { useForm, useWatch } from "react-hook-form";
import { FormInput } from "components/common/Form";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

export default function Chatbot() {
    const isOpen = useAppSelector(chatbotSelectors.isOpen);
    const isPinned = useAppSelector(chatbotSelectors.isPinned);
    const absoluteNotificationsWidth = useAppSelector(chatbotSelectors.absoluteNotificationsWidth);

    const resizable = useResizableWidth({
        initialWidth: 400,
        minWidth: 400,
        maxWidth: 600,
    });

    if (!isOpen) {
        return null;
    }

    const positionStyle: React.CSSProperties = isPinned
        ? { position: "relative" }
        : { position: "absolute", right: 10 + absoluteNotificationsWidth, top: 10, bottom: 10 };

    return (
        <div
            className={classNames("chatbot panel-bg-1 border-secondary vstack", {
                "h-100 border-left": isPinned,
                "border rounded-2": !isPinned,
            })}
            style={{
                ...positionStyle,
                width: `${resizable.width}px`,
                borderLeft: `1px solid ${resizable.isDragging ? "#ccc" : "#4c4c63"}`,
            }}
        >
            <ColumnResize handleMouseDown={resizable.handleMouseDown} />
            <Header />
            <ChatbotBody />
            <Footer />
        </div>
    );
}

function ChatbotBody() {
    const dispatch = useAppDispatch();
    const messages = useAppSelector(chatbotSelectors.messages);

    const { control, handleSubmit, formState, reset } = useForm({
        defaultValues: {
            prompt: "",
        },
    });

    const formValues = useWatch({
        control,
    });

    const handleSend = async () => {
        await dispatch(chatbotActions.runChat({ message: formValues.prompt, isContinuation: true })).unwrap();
        reset();
    };

    return (
        <div className="vstack flex-grow-1 p-2">
            <ChatbotMessages messages={messages} />
            <div className="position-relative">
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name="prompt"
                    placeholder="Ask the agent anything"
                    className="prompt-textarea"
                    onKeyDown={(e) => {
                        if (e.key === "Enter" && !e.shiftKey) {
                            e.preventDefault();
                            handleSubmit(handleSend)();
                        }
                    }}
                />
                {formValues.prompt && (
                    <ButtonWithSpinner
                        variant="secondary"
                        icon="arrow-up"
                        onClick={handleSubmit(handleSend)}
                        className="position-absolute rounded-pill"
                        style={{ right: "10px", bottom: "10px", zIndex: 5 }}
                        isSpinning={formState.isSubmitting}
                    />
                )}
            </div>
        </div>
    );
}

function Header() {
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
                <Icon icon="ai" />
                Ask AI
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

function Footer() {
    const dispatch = useAppDispatch();

    const activeTab = useAppSelector(chatbotSelectors.activeTab);
    const isPinned = useAppSelector(chatbotSelectors.isPinned);

    return (
        <div
            className={classNames("panel-bg-2 border-top border-secondary p-2 hstack", {
                "rounded-bottom-2": !isPinned,
            })}
        >
            <FooterItem
                icon="ai"
                title="Ask AI"
                isActive={activeTab === "askAi"}
                handleClick={() => dispatch(chatbotActions.activeTabSet("askAi"))}
            />
            <FooterItem
                icon="document2"
                title="What's new"
                isActive={activeTab === "whatsNew"}
                handleClick={() => dispatch(chatbotActions.activeTabSet("whatsNew"))}
            />
            <FooterItem
                icon="document"
                title="News"
                isActive={activeTab === "news"}
                handleClick={() => dispatch(chatbotActions.activeTabSet("news"))}
            />
            <FooterItem
                icon="resources"
                title="Resources"
                isActive={activeTab === "resources"}
                handleClick={() => dispatch(chatbotActions.activeTabSet("resources"))}
            />
        </div>
    );
}

interface FooterItemProps {
    icon: IconName;
    title: string;
    isActive: boolean;
    handleClick: () => void;
}

function FooterItem({ icon, title, isActive, handleClick }: FooterItemProps) {
    return (
        <div
            className={classNames("rounded-2 px-3 py-1 vstack align-items-center justify-content-center", {
                "panel-bg-3 border border-secondary": isActive,
                "cursor-pointer": !isActive,
            })}
            onClick={handleClick}
        >
            <div>
                <Icon icon={icon} margin="m-0" />
            </div>
            <div>{title}</div>
        </div>
    );
}
