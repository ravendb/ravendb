import { EmptySet } from "components/common/EmptySet";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { UseFieldArrayReturn, Control } from "react-hook-form";
import { EditAiAgentFormData, ParameterAiAgentFormData } from "../utils/editAiAgentValidation";
import { FormLabel, FormGroup } from "components/common/Form";
import Button from "react-bootstrap/Button";
import Table from "react-bootstrap/Table";
import OptionalLabel from "components/common/OptionalLabel";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import InnerForm from "components/common/InnerForm";

interface EditAiAgentParametersSectionProps {
    control: Control<ParameterAiAgentFormData>;
    handleSubmit: () => void;
    parametersFieldArray: UseFieldArrayReturn<EditAiAgentFormData, "parameters", "id">;
}

export default function EditAiAgentParametersSection({
    control,
    parametersFieldArray,
    handleSubmit,
}: EditAiAgentParametersSectionProps) {
    return (
        <>
            <h3 className="m-0 mt-3">
                Set agent parameters
                <PopoverWithHoverWrapper
                    message={
                        <>
                            Define query parameters that the agent will replace with fixed values before executing a
                            query tool against the database. This ensures that queries run only within the allowed data
                            scope. <br />
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
            <InnerForm onSubmit={handleSubmit}>
                <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                    <FormGroup>
                        <div className="d-flex gap-2">
                            <FormGroup className="w-25">
                                <FormLabel>Name</FormLabel>
                                <FormInput type="text" control={control} name="nameInput" placeholder="e.g. company" />
                            </FormGroup>
                            <FormGroup className="w-75">
                                <FormLabel>
                                    Description <OptionalLabel />
                                </FormLabel>
                                <FormInput
                                    type="text"
                                    control={control}
                                    name="descriptionInput"
                                    placeholder="e.g. The company ID"
                                />
                            </FormGroup>
                        </div>
                        <div className="d-flex justify-content-end">
                            <Button variant="info" onClick={handleSubmit}>
                                <Icon icon="plus" />
                                Add parameter
                            </Button>
                        </div>
                    </FormGroup>
                    <FormGroup>
                        <FormLabel>Parameters</FormLabel>
                        {parametersFieldArray.fields.length === 0 ? (
                            <EmptySet compact className="text-muted">
                                No parameters have been defined yet
                            </EmptySet>
                        ) : (
                            <div className="overflow-y-auto" style={{ maxHeight: "220px" }}>
                                <Table striped>
                                    <thead>
                                        <tr>
                                            <th>Name</th>
                                            <th>Description</th>
                                            <th style={{ width: "50px" }}></th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {parametersFieldArray.fields.map((field, index) => (
                                            <tr key={field.id}>
                                                <td>{field.name}</td>
                                                <td className="text-truncate" style={{ maxWidth: "300px" }}>
                                                    {field.description || "-"}
                                                </td>
                                                <td>
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
                    </FormGroup>
                </div>
            </InnerForm>
        </>
    );
}
