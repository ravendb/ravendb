import { EmptySet } from "components/common/EmptySet";
import {
    FormErrorIcon,
    FormGroup,
    FormInput,
    FormLabel,
    FormSelect,
    FormSwitch,
    OptionalLabel,
} from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useFormContext } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import useBoolean from "components/hooks/useBoolean";
import Collapse from "react-bootstrap/Collapse";
import CollapseButton from "components/common/CollapseButton";
import { SelectOption } from "components/common/select/Select";
import useEditAiAgentParametersSection from "../hooks/useEditAiAgentParametersSection";

export default function EditAiAgentParametersSection() {
    const { control } = useFormContext<EditAiAgentFormData>();
    const parametersEditor = useEditAiAgentParametersSection();

    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    return (
        <>
            <div className="hstack mt-3">
                <h3 className="m-0">
                    Set agent parameters
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Define query parameters that the agent will replace with fixed values before executing a
                                query tool against the database. This ensures that queries run only within the allowed
                                data scope.
                                <br />
                                <br />
                                You will need to provide values for these parameters when starting a new chat with the
                                agent.
                            </>
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["parameters"]} onError={() => setIsPanelOpen(true)} />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">
                Define query parameters that the agent will replace with fixed values before executing a query tool
                against the database.
            </div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-secondary border border-secondary small">
                                    <Icon icon="settings" margin="m-0" className="text-body" />
                                </div>
                                <div>Agent parameters</div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={parametersEditor.handleAdd}>
                                <Icon icon="plus" />
                                Add new parameter
                            </Button>
                        </div>
                        <div className="vstack">
                            {parametersEditor.fieldArray.fields.length === 0 ? (
                                <div className="well p-2 rounded-2 border border-secondary mt-2 d-flex justify-content-center align-items-center">
                                    <EmptySet compact className="text-muted">
                                        No parameters have been defined yet
                                    </EmptySet>
                                </div>
                            ) : (
                                <div className="parameters-cards vstack">
                                    {parametersEditor.fieldArray.fields.map((field, index) => (
                                        <ParameterItem
                                            key={field.id}
                                            index={index}
                                            parameterItem={parametersEditor.parameters[index]}
                                            remove={() => parametersEditor.handleRemove(index)}
                                            save={() => parametersEditor.handleSave(index)}
                                            edit={() => parametersEditor.handleEdit(index)}
                                        />
                                    ))}
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </Collapse>
        </>
    );
}

interface ParameterItemProps {
    index: number;
    parameterItem: EditAiAgentFormData["parameters"][number];
    remove: () => void;
    save: () => Promise<void>;
    edit: () => void;
}

function ParameterItem({ index, parameterItem, remove, save, edit }: ParameterItemProps) {
    const { control } = useFormContext<EditAiAgentFormData>();

    if (!parameterItem) {
        return null;
    }

    if (!parameterItem.isEditing) {
        return (
            <div className="well p-2 rounded-2 border border-secondary mt-2 d-flex justify-content-between align-items-center gap-2">
                <div className="parameter-summary min-width-0 flex-grow-1">
                    <div className="d-flex align-items-center gap-1 min-width-0">
                        <h4 className="m-0 text-truncate" title={parameterItem.name}>
                            {parameterItem.name}
                        </h4>
                        <span className="small text-secondary">|</span>
                        <span className="fs-4 flex-shrink-0 text-monospace text-muted">
                            {getParameterTypeLabel(parameterItem.type)}
                        </span>
                    </div>
                    <small className="d-block text-truncate" title={parameterItem.description}>
                        {parameterItem.description}
                    </small>
                </div>
                <div className="hstack gap-2 flex-shrink-0">
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
                <h4 className="m-0">Configure parameter</h4>
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
            <div className="grid gap-2 mb-3">
                <FormGroup marginClass="m-0" className="g-col-6">
                    <FormLabel>Parameter name</FormLabel>
                    <FormInput
                        type="text"
                        control={control}
                        name={`parameters.${index}.name`}
                        placeholder="e.g. company"
                    />
                </FormGroup>
                <FormGroup marginClass="m-0" className="g-col-6">
                    <FormLabel>Parameter type</FormLabel>
                    <FormSelect
                        control={control}
                        name={`parameters.${index}.type`}
                        options={typeOptions}
                        placeholder="Select a type"
                        isSearchable={false}
                        isClearable={false}
                    />
                </FormGroup>
            </div>
            <FormGroup>
                <FormLabel>
                    Description <OptionalLabel />
                </FormLabel>
                <FormInput
                    type="textarea"
                    as="textarea"
                    rows={3}
                    control={control}
                    name={`parameters.${index}.description`}
                    placeholder="e.g. The company ID"
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    Forbid model generation
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                When <b>Forbid model generation</b> is set and this agent is used as a sub-agent, the
                                parent agent is not allowed to generate a parameter value for the sub-agent.
                                <br />
                                The parameter&apos;s value may only be inherited from the parent agent parameters. This
                                ensures that this is a trusted value.
                            </>
                        }
                    >
                        <Icon icon="info-new" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelect
                    control={control}
                    name={`parameters.${index}.policy`}
                    options={policyOptions}
                    placeholder="Select a value"
                    isSearchable={false}
                    isClearable={false}
                />
            </FormGroup>
            <FormSwitch control={control} name={`parameters.${index}.isSendToModel`}>
                Send to model
                <PopoverWithHoverWrapper
                    message={
                        <>
                            When enabled, the parameter is exposed to the model. It is included in the prompt sent to
                            the model and in any echo messages.
                            <br />
                            <br />
                            When disabled, the parameter is hidden from the model. It is excluded from both the prompt
                            and echo messages.
                        </>
                    }
                >
                    <Icon icon="info-new" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormSwitch>
        </div>
    );
}

function getParameterTypeLabel(type: Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType) {
    return typeOptions.find((option) => option.value === type)?.label ?? type;
}

const typeOptions: SelectOption<Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType>[] = [
    { value: "String", label: "String" },
    { value: "Number", label: "Number" },
    { value: "Boolean", label: "Boolean" },
    { value: "ArrayOfString", label: "String[]" },
    { value: "ArrayOfNumber", label: "Number[]" },
    { value: "ArrayOfBoolean", label: "Boolean[]" },
    { value: "Default", label: "Any" },
    { value: "Null", label: "Null" },
];

const policyOptions: SelectOption<Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterPolicy>[] = [
    { value: "Default", label: "Default" },
    { value: "ForbidModelGeneration", label: "Forbid model generation" },
];
