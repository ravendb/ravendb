import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import useEditAiAgentToolsSection from "../hooks/useEditAiAgentToolsSection";
import EditAiAgentQueryToolItem from "./EditAiAgentQueryToolItem";
import EditAiAgentActionToolItem from "./EditAiAgentActionToolItem";

export default function EditAiAgentToolsSection() {
    const toolsEditor = useEditAiAgentToolsSection();

    return (
        <>
            <h3 className="m-0 mt-3">
                Define agent tools
                <PopoverWithHoverWrapper
                    message={
                        <>
                            Tools are a controlled way to pass context to the LLM. Configure the tools that the LLM can
                            instruct the agent to trigger in response to user prompts.
                            <br />
                            <br />
                            These include query tools (to retrieve data from the database) and action tools (to initiate
                            tasks that are expected to be carried out by the client or user).
                        </>
                    }
                >
                    <Icon icon="info" color="info" margin="ms-1" className="fs-3" />
                </PopoverWithHoverWrapper>
            </h3>
            <div className="mb-1">Tools are a controlled way to pass context to the LLM.</div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <div className="hstack justify-content-between">
                    <div className="hstack gap-2">
                        <div className="p-1 rounded-2 bg-faded-primary border border-primary">
                            <Icon icon="query" color="primary" margin="m-0" />
                        </div>
                        <div>
                            Query tools
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        Define queries the agent is allowed to execute against the database in order to
                                        retrieve data.
                                        <br />
                                        <br />
                                        The LLM can instruct the agent to run these queries as needed to answer user
                                        questions.
                                        <br />
                                        <br />
                                        You can restrict the query scope by filtering results using the defined
                                        &quot;agent parameters&quot;.
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" />
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
                            cancelEdit={() => toolsEditor.handleCancelEditQuery(index)}
                        />
                    ))}
                </div>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary mt-2">
                <div className="hstack justify-content-between">
                    <div className="hstack gap-2">
                        <div className="p-1 rounded-2 bg-faded-primary border border-primary">
                            <Icon icon="force" color="primary" margin="m-0" />
                        </div>
                        <div>
                            Action tools
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        Define actions that the agent can trigger when requested by the LLM, allowing
                                        the backend or client to perform operations in response to user prompts and
                                        conversation context.
                                        <br />
                                        <br />
                                        Each action tool should handle a specific task in your system - for example,
                                        creating a support ticket, sending a notification, or updating a document
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" />
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
                            cancelEdit={() => toolsEditor.handleCancelEditAction(index)}
                        />
                    ))}
                </div>
            </div>
        </>
    );
}
