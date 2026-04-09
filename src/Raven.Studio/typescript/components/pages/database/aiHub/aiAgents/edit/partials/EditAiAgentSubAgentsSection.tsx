import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import useBoolean from "components/hooks/useBoolean";
import CollapseButton from "components/common/CollapseButton";
import Collapse from "react-bootstrap/Collapse";
import { FormErrorIcon, FormGroup, FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import useEditAiAgentSubAgentsSection from "components/pages/database/aiHub/aiAgents/edit/hooks/useEditAiAgentSubAgentsSection";
import { useAppSelector } from "components/store";
import { editAiAgentSelectors } from "components/pages/database/aiHub/aiAgents/edit/store/editAiAgentSlice";
import { SelectOption } from "components/common/select/Select";

export default function EditAiAgentSubAgentsSection() {
    const { control } = useFormContext<EditAiAgentFormData>();
    const { fieldArray, handleAdd, handleSave, handleEdit, handleRemove } = useEditAiAgentSubAgentsSection();

    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    return (
        <>
            <div className="hstack mt-3">
                <h3 className="m-0">
                    Define sub-agents
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Sub-agents are other AI agents that this parent agent can trigger when requested by the
                                LLM to handle specialized tasks.
                                <br />
                                <br />
                                When the LLM determines it should delegate work to a sub-agent, it generates a
                                natural-language prompt for that sub-agent. The parent agent then invokes the sub-agent
                                and forwards the prompt. The sub-agent runs its own tools (queries, actions) to complete
                                the task and returns its response to the parent agent.
                            </>
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["subAgents"]} onError={() => setIsPanelOpen(true)} />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">Choose existing agents to act as sub-agents for this one.</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-primary border border-primary small">
                                    <Icon icon="sub-agents" color="primary" margin="m-0" />
                                </div>
                                <div>
                                    Sub-agents
                                    <PopoverWithHoverWrapper
                                        message={
                                            <>
                                                Add the agents that this agent is allowed to delegate work to. The
                                                description you provide tells the LLM when it should trigger that
                                                specific sub-agent.
                                            </>
                                        }
                                    >
                                        <Icon icon="info-new" />
                                    </PopoverWithHoverWrapper>
                                </div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={handleAdd}>
                                <Icon icon="plus" />
                                Add new sub-agent
                            </Button>
                        </div>
                        <div className="vstack">
                            {fieldArray.fields.map((field, index) => (
                                <SubAgentItem
                                    key={field.id}
                                    index={index}
                                    remove={() => handleRemove(index)}
                                    save={() => handleSave(index)}
                                    edit={() => handleEdit(index)}
                                />
                            ))}
                        </div>
                    </div>
                </div>
            </Collapse>
        </>
    );
}

interface SubAgentItemProps {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
}

function SubAgentItem({ index, remove, save, edit }: SubAgentItemProps) {
    const { control } = useFormContext<EditAiAgentFormData>();

    const currentIdentifier = useWatch({
        control,
        name: "identifier",
    });

    const subAgents = useWatch({
        control,
        name: "subAgents",
    });

    const allIdentifiersOptions: SelectOption[] = useAppSelector(editAiAgentSelectors.allIdentifiers)
        .filter((id) => id !== currentIdentifier)
        .map((id) => ({
            label: id,
            value: id,
        }));

    const subAgentItem = subAgents[index];

    const isAgentNotFound =
        subAgentItem.identifier && !allIdentifiersOptions.some((option) => option.value === subAgentItem.identifier);

    if (!subAgentItem.isEditing) {
        return (
            <div className="well p-2 rounded-2 border border-secondary mt-2 hstack justify-content-between align-items-center gap-3">
                <div className="tool-info">
                    <h4 className="m-0">{subAgentItem.identifier}</h4>
                    <small className="tool-description">{subAgentItem.description}</small>
                </div>
                <div className="hstack gap-2">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={edit}>
                        <Icon icon="chevron-down" margin="m-0" />
                    </Button>
                </div>
            </div>
        );
    }

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">Configure sub-agent</h4>
                <div className="hstack gap-2">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={save}>
                        <Icon icon="chevron-up" margin="m-0" />
                    </Button>
                </div>
            </div>
            <hr className="mt-2 mb-3" />
            <FormGroup>
                <FormLabel>Agent identifier</FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name={`subAgents.${index}.identifier`}
                    options={allIdentifiersOptions}
                />
                {isAgentNotFound && (
                    <div className="text-warning mt-1 fw-bold">
                        Warning: This agent is currently not found in your database
                    </div>
                )}
            </FormGroup>
            <FormGroup>
                <FormLabel>Description</FormLabel>
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name={`subAgents.${index}.description`}
                    rows={4}
                    placeholder="In this description, explain to the LLM when it should delegate to this sub-agent.
                                 E.g., Use this agent to handle all questions about orders, order history, and order status."
                />
            </FormGroup>
        </div>
    );
}
