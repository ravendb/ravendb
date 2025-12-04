import { EmptySet } from "components/common/EmptySet";
import { FormInput, FormErrorIcon, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useFormContext, useFieldArray } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import OptionalLabel from "components/common/OptionalLabel";
import useBoolean from "components/hooks/useBoolean";
import Collapse from "react-bootstrap/Collapse";
import CollapseButton from "components/common/CollapseButton";

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
                        <div className="d-flex justify-content-end">
                            <Button
                                variant="link"
                                size="sm"
                                onClick={() =>
                                    parametersFieldArray.append({ name: "", description: null, isSendToModel: true })
                                }
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
                                <div className="parameters-table-wrapper">
                                    <table className="table table-borderless">
                                        <thead>
                                            <tr>
                                                <th>Parameter</th>
                                                <th>
                                                    Description <OptionalLabel />
                                                </th>
                                                <th style={{ width: "135px" }}>
                                                    Send to model
                                                    <PopoverWithHoverWrapper
                                                        message={
                                                            <>
                                                                When enabled, the parameter is exposed to the model.
                                                                <br />
                                                                <br />
                                                                When disabled, the parameter is hidden from the model
                                                                (it will not be included in prompts/echo messages).
                                                            </>
                                                        }
                                                    >
                                                        <Icon icon="info-new" />
                                                    </PopoverWithHoverWrapper>
                                                </th>
                                                <th style={{ width: "80px" }}>Actions</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {parametersFieldArray.fields.map((field, index) => (
                                                <tr key={field.id}>
                                                    <td>
                                                        <FormInput
                                                            type="text"
                                                            control={control}
                                                            name={`parameters.${index}.name`}
                                                            placeholder="e.g. company"
                                                            className="form-control border-0 rounded-0"
                                                        />
                                                    </td>
                                                    <td>
                                                        <FormInput
                                                            type="text"
                                                            control={control}
                                                            name={`parameters.${index}.description`}
                                                            placeholder="e.g. The company ID"
                                                            className="form-control border-0 rounded-0"
                                                        />
                                                    </td>
                                                    <td>
                                                        <div className="hstack justify-content-center">
                                                            <FormSwitch
                                                                control={control}
                                                                name={`parameters.${index}.isSendToModel`}
                                                            />
                                                        </div>
                                                    </td>
                                                    <td className="text-center">
                                                        <Button
                                                            variant="link"
                                                            className="text-danger p-0"
                                                            size="sm"
                                                            onClick={() => parametersFieldArray.remove(index)}
                                                            title="Delete this parameter"
                                                        >
                                                            <Icon icon="trash" margin="m-0" />
                                                        </Button>
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </Collapse>
        </div>
    );
}
