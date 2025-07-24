import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import { Control, FieldPath, FieldValues } from "react-hook-form";

interface AiAgentParametersFieldProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    name: TName;
    value: { name?: string; value?: string }[];
}

export default function AiAgentParametersField<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
>({ control, name, value }: AiAgentParametersFieldProps<TFieldValues, TName>) {
    if (value.length === 0) {
        return null;
    }

    return (
        <div className="text-center w-100 overflow-auto py-2">
            <Icon icon="metrics" color="primary" size="lg" />
            <h3 className="mt-1">To start a new chat select a chosen parameters.</h3>

            {value.map((x, idx) => (
                <div key={x.name} className="w-100 p-2">
                    <div className="hstack">
                        <Badge
                            bg="primary"
                            className="text-truncate me-2 fs-5"
                            title={x.name}
                            style={{ width: "150px" }}
                            pill
                        >
                            {x.name}
                        </Badge>
                        <FormInput
                            type="text"
                            control={control}
                            name={`${name}.${idx}.value` as TName}
                            placeholder={`<${x.name} id>`}
                        />
                    </div>
                    {idx !== value.length - 1 && <hr className="my-1" />}
                </div>
            ))}
        </div>
    );
}
