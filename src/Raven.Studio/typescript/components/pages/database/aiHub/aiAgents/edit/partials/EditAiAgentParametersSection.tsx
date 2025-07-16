import { EmptySet } from "components/common/EmptySet";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useFormContext, useFieldArray, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { FormLabel, FormGroup } from "components/common/Form";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";

export default function EditAiAgentParametersSection() {
    const { control, setValue, trigger } = useFormContext<EditAiAgentFormData>();

    const parametersFieldArray = useFieldArray({
        name: "parameters",
        control,
    });

    const formValues = useWatch({
        control,
    });

    const handleAddParameter = async () => {
        const isValid = await trigger("parameterInput");
        if (!isValid || !formValues.parameterInput) {
            return;
        }

        parametersFieldArray.append({ name: formValues.parameterInput });
        setValue("parameterInput", "");
    };

    return (
        <>
            <h3 className="m-0 mt-3">Set agent parameters</h3>
            <div className="mb-1">
                Create parameters to control and restrict data that you want your agent to have access to.
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>Define a parameter</FormLabel>
                    <FormInput
                        type="text"
                        control={control}
                        name="parameterInput"
                        placeholder="e.g. company"
                        addon={
                            <Button variant="link" className="text-reset" onClick={handleAddParameter}>
                                <Icon icon="plus" />
                                Add parameter
                            </Button>
                        }
                    />
                </FormGroup>
                <FormGroup>
                    <FormLabel>List of parameters</FormLabel>
                    {parametersFieldArray.fields.length === 0 ? (
                        <EmptySet compact className="text-muted">
                            No parameters have been defined yet
                        </EmptySet>
                    ) : (
                        <div className="d-flex gap-2 flex-wrap">
                            {parametersFieldArray.fields.map((field, index) => (
                                <Badge key={field.id} bg="primary" pill>
                                    {field.name}
                                    <Button
                                        variant="link"
                                        className="p-0"
                                        onClick={() => parametersFieldArray.remove(index)}
                                        size="xs"
                                    >
                                        <Icon icon="trash" margin="m-0" color="light" />
                                    </Button>
                                </Badge>
                            ))}
                        </div>
                    )}
                </FormGroup>
            </div>
        </>
    );
}
