import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import useEditAiAgentToolsSection from "../hooks/useEditAiAgentToolsSection";
import EditAiAgentQueryToolItem from "./EditAiAgentQueryToolItem";
import EditAiAgentActionToolItem from "./EditAiAgentActionToolItem";
import useBoolean from "components/hooks/useBoolean";
import CollapseButton from "components/common/CollapseButton";
import Collapse from "react-bootstrap/Collapse";
import { FormErrorIcon } from "components/common/Form";
import { useFormContext } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";

export default function EditAiAgentToolsSection() {
    const { control } = useFormContext<EditAiAgentFormData>();
    const toolsEditor = useEditAiAgentToolsSection();

    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    return (
        <>
            <div className="hstack mt-3">
                <h3 className="m-0">
                    Define agent tools
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Tools are a controlled way to pass context to the LLM. Configure the tools that the LLM
                                can instruct the agent to trigger in response to user prompts.
                                <br />
                                <br />
                                These include query tools (to retrieve data from the database) and action tools (to
                                initiate tasks that are expected to be carried out by the client or user).
                            </>
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["queries", "actions"]} onError={() => setIsPanelOpen(true)} />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">Tools are a controlled way to pass context to the LLM.</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-primary border border-primary">
                                    <Icon icon="query" color="primary" margin="m-0" />
                                </div>
                                <div>
                                    Query tools
                                    <PopoverWithHoverWrapper
                                        message={
                                            <>
                                                Define queries the agent is allowed to execute against the database in
                                                order to retrieve data.
                                                <br />
                                                <br />
                                                The LLM can instruct the agent to run these queries as needed to answer
                                                user questions.
                                                <br />
                                                <br />
                                                You can restrict the query scope by filtering results using the defined
                                                &quot;agent parameters&quot;.
                                            </>
                                        }
                                    >
                                        <Icon icon="info-new" />
                                    </PopoverWithHoverWrapper>
                                </div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={toolsEditor.handleAddQuery}>
                                <Icon icon="plus" />
                                Add new query tool
                            </Button>
                        </div>
                        <div className="vstack">
                            {toolsEditor.queriesFieldArray.fields.map((field, index) => (
                                <EditAiAgentQueryToolItem
                                    key={field.id}
                                    index={index}
                                    remove={() => toolsEditor.handleRemoveQuery(index)}
                                    save={() => toolsEditor.handleSaveQuery(index)}
                                    edit={() => toolsEditor.handleEditQuery(index)}
                                />
                            ))}
                        </div>
                    </div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary mt-2">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-primary border border-primary">
                                    <Icon icon="force" color="primary" margin="m-0" />
                                </div>
                                <div>
                                    Action tools
                                    <PopoverWithHoverWrapper
                                        message={
                                            <>
                                                Define actions that the agent can trigger when requested by the LLM,
                                                allowing the backend or client to perform operations in response to user
                                                prompts and conversation context.
                                                <br />
                                                <br />
                                                Each action tool should handle a specific task in your system - for
                                                example, creating a support ticket, sending a notification, or updating
                                                a document
                                            </>
                                        }
                                    >
                                        <Icon icon="info-new" />
                                    </PopoverWithHoverWrapper>
                                </div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={toolsEditor.handleAddAction}>
                                <Icon icon="plus" />
                                Add new action tool
                            </Button>
                        </div>
                        <div className="vstack">
                            {toolsEditor.actionsFieldArray.fields.map((field, index) => (
                                <EditAiAgentActionToolItem
                                    key={field.id}
                                    index={index}
                                    remove={() => toolsEditor.handleRemoveAction(index)}
                                    save={() => toolsEditor.handleSaveAction(index)}
                                    edit={() => toolsEditor.handleEditAction(index)}
                                />
                            ))}
                        </div>
                    </div>
                </div>
            </Collapse>
        </>
    );
}
