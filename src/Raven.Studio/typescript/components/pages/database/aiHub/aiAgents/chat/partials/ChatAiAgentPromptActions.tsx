import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { UseFieldArrayReturn } from "react-hook-form";
import { ChatAiAgentFormData } from "../utils/chatAiAgentValidation";
import Button from "react-bootstrap/Button";
import "./ChatAiAgentFormBody.scss";
import { Icon } from "components/common/Icon";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useAppDispatch, useAppSelector } from "components/store";
import { chatAiAgentActions, chatAiAgentSelectors } from "../store/chatAiAgentSlice";
import ChatAiAgentAttachmentsDropdown from "./ChatAiAgentAttachmentsDropdown";

interface ChatAiAgentPromptActionsProps {
    isPromptDisabled: boolean;
    isLoading: boolean;
    hasPromptErrors: boolean;
    promptsFieldsArray: UseFieldArrayReturn<ChatAiAgentFormData, "prompts", "id">;
    attachmentsFieldsArray: UseFieldArrayReturn<ChatAiAgentFormData, "attachments", "id">;
}

export default function ChatAiAgentPromptActions({
    isPromptDisabled,
    isLoading,
    hasPromptErrors,
    promptsFieldsArray,
    attachmentsFieldsArray,
}: ChatAiAgentPromptActionsProps) {
    const dispatch = useAppDispatch();
    const activePromptIndex = useAppSelector(chatAiAgentSelectors.activePromptIndex);

    const setActivePromptIndex = (index: number) => {
        dispatch(chatAiAgentActions.activePromptIndexSet(index));
    };

    const promptsCount = promptsFieldsArray.fields.length;

    return (
        <div className="hstack gap-1 prompt-actions">
            <ChatAiAgentAttachmentsDropdown
                attachmentsFieldsArray={attachmentsFieldsArray}
                isPromptDisabled={isPromptDisabled}
            />
            {hasPromptErrors && (
                <PopoverWithHoverWrapper message={promptsCount > 1 ? "Prompts are required" : "Prompt is required"}>
                    <Icon icon="warning" color="danger" margin="m-0" />
                </PopoverWithHoverWrapper>
            )}
            {promptsCount > 1 && (
                <div className="hstack">
                    <Button
                        variant="link"
                        onClick={() => setActivePromptIndex(activePromptIndex - 1)}
                        disabled={activePromptIndex === 0}
                        className="ps-0"
                    >
                        <Icon icon="chevron-left" margin="m-0" />
                    </Button>
                    <span>
                        {activePromptIndex + 1} of {promptsCount}
                    </span>
                    <Button
                        variant="link"
                        onClick={() => setActivePromptIndex(activePromptIndex + 1)}
                        disabled={activePromptIndex === promptsCount - 1}
                        className="pe-0"
                    >
                        <Icon icon="chevron-right" margin="m-0" />
                    </Button>
                </div>
            )}
            <Dropdown>
                <Dropdown.Toggle as={CustomDropdownToggle} isCaretHidden variant="link" disabled={isPromptDisabled}>
                    <Icon icon="more" margin="m-0" />
                </Dropdown.Toggle>
                <Dropdown.Menu>
                    <Dropdown.Item
                        title="Add prompt"
                        onClick={() => {
                            setActivePromptIndex(promptsCount);
                            promptsFieldsArray.append({ text: "" });
                        }}
                    >
                        <Icon icon="plus" />
                        <span>Add another prompt</span>
                    </Dropdown.Item>
                    {promptsCount > 1 && (
                        <Dropdown.Item
                            title="Remove prompt"
                            onClick={() => {
                                if (activePromptIndex !== 0) {
                                    setActivePromptIndex(activePromptIndex - 1);
                                }

                                promptsFieldsArray.remove(activePromptIndex);
                            }}
                        >
                            <Icon icon="trash" />
                            <span>Remove prompt</span>
                        </Dropdown.Item>
                    )}
                </Dropdown.Menu>
            </Dropdown>
            <ButtonWithSpinner
                type="submit"
                variant="secondary"
                icon="arrow-thin-top"
                isSpinning={isLoading}
                disabled={isPromptDisabled}
                className="rounded-pill"
            />
        </div>
    );
}
