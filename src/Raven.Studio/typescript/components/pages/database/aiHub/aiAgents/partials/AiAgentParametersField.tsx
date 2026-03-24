import classNames from "classnames";
import { FormInput, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Badge from "react-bootstrap/Badge";
import { Control } from "react-hook-form";
import { TestAiAgentFormData } from "../edit/utils/editAiAgentValidation";
import { ChatAiAgentFormData } from "../chat/utils/chatAiAgentValidation";
import assertUnreachable from "components/utils/assertUnreachable";

interface Parameter {
    name?: string;
    value?: string;
    type?: Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType;
}

type FormData = TestAiAgentFormData | ChatAiAgentFormData;

interface AiAgentParametersFieldProps {
    control: Control<FormData>;
    value: Parameter[];
    wrapperClassName?: string;
    headerClassName?: string;
    panelClassName?: string;
}

export default function AiAgentParametersField({
    control,
    value,
    wrapperClassName,
    headerClassName,
    panelClassName,
}: AiAgentParametersFieldProps) {
    if (value.length === 0) {
        return null;
    }

    return (
        <div className={classNames("w-100 vstack flex-grow-1 overflow-auto", wrapperClassName)}>
            <h3 className={classNames(headerClassName)}>
                <Icon icon="metrics" color="primary" />
                Provide values for the agent parameters before starting the chat
                <PopoverWithHoverWrapper message="When a query tool is used, the agent will insert these values into the query definition before executing it against the database, filtering the data and ensuring the query only retrieves data within the allowed scope.">
                    <Icon icon="info-new" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </h3>
            <div className="overflow-auto d-grid gap-2 mb-2">
                {value.map((parameter, idx) => (
                    <ParameterItem
                        key={idx}
                        control={control}
                        parameter={parameter}
                        idx={idx}
                        panelClassName={panelClassName}
                    />
                ))}
            </div>
        </div>
    );
}

interface ParameterItemProps {
    control: Control<FormData>;
    parameter: Parameter;
    idx: number;
    panelClassName?: string;
}

function ParameterItem({ control, parameter, idx, panelClassName }: ParameterItemProps) {
    const typeInfo = getParameterTypeInfo(parameter.type);

    if (parameter.type === "Null") {
        return null;
    }

    return (
        <div className={classNames("p-2 rounded-2 d-grid gap-1 border border-secondary", panelClassName)}>
            <div className="d-flex justify-content-between align-items-end">
                <Badge
                    bg="primary"
                    className="font-size-12 text-truncate"
                    title={parameter.name}
                    pill
                    style={{ maxWidth: "300px" }}
                >
                    {parameter.name}
                </Badge>
                <FormSwitch control={control} name={`parameters.${idx}.isSendToModel`} color="primary" className="mb-0">
                    Send to model
                </FormSwitch>
            </div>
            <FormInput
                type={parameter.type === "Number" ? "number" : "text"}
                control={control}
                name={`parameters.${idx}.value`}
                placeholder={`Enter a value for <${parameter.name}> ${typeInfo.exampleValue ? `(e.g. ${typeInfo.exampleValue})` : ""}`}
                addon={typeInfo.label}
            />
        </div>
    );
}

interface ParameterTypeInfo {
    label: string;
    exampleValue?: string;
}

function getParameterTypeInfo(
    type: Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType
): ParameterTypeInfo {
    switch (type) {
        case "ArrayOfString":
            return { label: "String[]", exampleValue: `["value1", "value2", "value3"]` };
        case "ArrayOfNumber":
            return { label: "Number[]", exampleValue: "[1, 2, 3]" };
        case "ArrayOfBoolean":
            return { label: "Boolean[]", exampleValue: "[true, false, true]" };
        case "String":
        case "Number":
        case "Boolean":
        case "Null":
        case "Default":
            return { label: type };
        default:
            assertUnreachable(type);
    }
}
