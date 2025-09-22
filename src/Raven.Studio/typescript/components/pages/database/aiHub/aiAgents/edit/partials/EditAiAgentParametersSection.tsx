import { EmptySet } from "components/common/EmptySet";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useFormContext, useFieldArray } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import OptionalLabel from "components/common/OptionalLabel";
import useBoolean from "components/hooks/useBoolean";
import EditAiAgentCollapseButton from "./EditAiAgentCollapseButton";
import EditAiAgentErrorIcon from "./EditAiAgentErrorIcon";
import Collapse from "react-bootstrap/Collapse";

export default function EditAiAgentParametersSection() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const parametersFieldArray = useFieldArray({
        control,
        name: "parameters",
    });

    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    return (
        <div className="edit-ai-agent-parameters-section">
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
                <EditAiAgentErrorIcon fieldNames={["parameters"]} openPanel={setIsPanelOpen} />
                <EditAiAgentCollapseButton isPanelOpen={isPanelOpen} toggleIsPanelOpen={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">
                Define query parameters that the agent will replace with fixed values before executing a query tool
                against the database.
            </div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <div className="d-flex justify-content-end">
                            <Button
                                variant="link"
                                size="sm"
                                onClick={() => parametersFieldArray.append({ name: "", description: null })}
                                className="mb-1"
                            >
                                <Icon icon="plus" />
                                Add new parameter
                            </Button>
                        </div>
                        <div>
                            {parametersFieldArray.fields.length === 0 ? (
                                <div className="panel-bg-2 d-flex justify-content-center align-items-center rounded-2 border border-secondary p-2">
                                    <EmptySet compact className="text-muted">
                                        No parameters have been defined yet
                                    </EmptySet>
                                </div>
                            ) : (
                                <div className="parameters-grid overflow-y-auto" style={{ maxHeight: "220px" }}>
                                    <div className="parameters-header position-sticky top-0 z-1 panel-bg-2">
                                        <div className="parameters-grid-header">
                                            <div className="parameter-name-header fw-bold">Parameter</div>
                                            <div className="parameter-description-header fw-bold">
                                                Description <OptionalLabel />
                                            </div>
                                            <div className="parameter-actions-header fw-bold">Actions</div>
                                        </div>
                                    </div>
                                    <div className="parameters-list">
                                        {parametersFieldArray.fields.map((field, index) => (
                                            <div key={field.id} className="parameter-row">
                                                <div className="parameters-grid-row">
                                                    <div className="parameter-name-cell">
                                                        <FormInput
                                                            type="text"
                                                            control={control}
                                                            name={`parameters.${index}.name`}
                                                            placeholder="e.g. company"
                                                            className="form-control border-0 rounded-0"
                                                        />
                                                    </div>
                                                    <div className="parameter-description-cell">
                                                        <FormInput
                                                            type="text"
                                                            control={control}
                                                            name={`parameters.${index}.description`}
                                                            placeholder="e.g. The company ID"
                                                            className="form-control border-0 rounded-0"
                                                        />
                                                    </div>
                                                    <div className="d-flex px-1">
                                                        <Button
                                                            variant="link"
                                                            className="text-danger p-0"
                                                            size="sm"
                                                            onClick={() => parametersFieldArray.remove(index)}
                                                            title="Delete this parameter"
                                                        >
                                                            <Icon icon="trash" margin="m-0" />
                                                        </Button>
                                                    </div>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </Collapse>
        </div>
    );
}
