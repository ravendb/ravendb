import { EmptySet } from "components/common/EmptySet";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useFormContext, useFieldArray } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { FormLabel } from "components/common/Form";
import Button from "react-bootstrap/Button";
import Table from "react-bootstrap/Table";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import OptionalLabel from "components/common/OptionalLabel";

export default function EditAiAgentParametersSection() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const parametersFieldArray = useFieldArray({
        control,
        name: "parameters",
    });

    return (
        <div className="edit-ai-agent-parameters-section">
            <h3 className="m-0 mt-3">
                Set agent parameters
                <PopoverWithHoverWrapper
                    message={
                        <>
                            Define query parameters that the agent will replace with fixed values before executing a
                            query tool against the database. This ensures that queries run only within the allowed data
                            scope.
                            <br />
                            <br />
                            You will need to provide values for these parameters when starting a new chat with the
                            agent.
                        </>
                    }
                >
                    <Icon icon="info" color="info" margin="ms-1" className="fs-3" />
                </PopoverWithHoverWrapper>
            </h3>
            <div className="mb-1">
                Define query parameters that the agent will replace with fixed values before executing a query tool
                against the database.
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <div className="d-flex justify-content-end">
                    <Button
                        variant="link"
                        size="sm"
                        onClick={() => parametersFieldArray.append({ name: "", description: null })}
                    >
                        <Icon icon="plus" />
                        Add new parameter
                    </Button>
                </div>
                <div>
                    <FormLabel>Parameters</FormLabel>
                    {parametersFieldArray.fields.length === 0 ? (
                        <div className="panel-bg-2 d-flex justify-content-center align-items-center rounded-2 border border-secondary p-2">
                            <EmptySet compact className="text-muted">
                                No parameters have been defined yet
                            </EmptySet>
                        </div>
                    ) : (
                        <div className="overflow-y-auto" style={{ maxHeight: "220px" }}>
                            <Table bordered>
                                <thead className="panel-bg-2">
                                    <tr>
                                        <th>Parameter</th>
                                        <th className="w-75">
                                            Description <OptionalLabel />
                                        </th>
                                        <th style={{ width: "50px" }}></th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {parametersFieldArray.fields.map((field, index) => (
                                        <tr key={field.id}>
                                            <td className="p-0">
                                                <FormInput
                                                    type="text"
                                                    control={control}
                                                    name={`parameters.${index}.name`}
                                                    placeholder="e.g. company"
                                                    className="rounded-0 border-0"
                                                />
                                            </td>
                                            <td className="w-75 p-0">
                                                <FormInput
                                                    type="text"
                                                    control={control}
                                                    name={`parameters.${index}.description`}
                                                    placeholder="e.g. The company ID"
                                                    className="rounded-0 border-0"
                                                />
                                            </td>
                                            <td className="align-middle">
                                                <Button
                                                    variant="link"
                                                    className="text-danger"
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
                            </Table>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
