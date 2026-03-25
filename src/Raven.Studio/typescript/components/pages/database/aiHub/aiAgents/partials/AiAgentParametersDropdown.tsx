import { CustomDropdownToggle } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import Dropdown from "react-bootstrap/Dropdown";
import Form from "react-bootstrap/Form";
import { aiAgentParametersUtils } from "../utils/aiAgentParametersUtils";

interface AiAgentParametersDropdownProps {
    parameters: Record<string, string | Raven.Client.Documents.AI.AiConversationParameter>;
    isCompact?: boolean;
}

export default function AiAgentParametersDropdown({ parameters, isCompact = false }: AiAgentParametersDropdownProps) {
    const parametersArray = Object.entries(parameters).map(([name, value]) => ({ name, value }));

    if (parametersArray.length === 0) {
        return null;
    }

    return (
        <Dropdown>
            <Dropdown.Toggle
                title="Parameters"
                variant="outline-secondary"
                className="rounded-pill"
                as={CustomDropdownToggle}
                isCaretHidden={isCompact}
            >
                <Icon icon="metrics" margin={isCompact ? "m-0" : "me-1"} />
                {isCompact ? null : "Parameters"}
            </Dropdown.Toggle>
            <Dropdown.Menu
                style={{ width: "500px", maxHeight: "316px" }}
                className="panel-bg-1 p-3 rounded-2 overflow-auto"
            >
                {parametersArray.map((x, idx) => (
                    <ParameterItem
                        key={x.name}
                        name={x.name}
                        value={x.value}
                        isLast={idx === parametersArray.length - 1}
                    />
                ))}
            </Dropdown.Menu>
        </Dropdown>
    );
}

interface ParameterItemProps {
    name: string;
    value: string | Raven.Client.Documents.AI.AiConversationParameter;
    isLast: boolean;
}

function ParameterItem({ name, value, isLast }: ParameterItemProps) {
    const extractedValue = typeof value === "object" && "Value" in value ? value.Value : value;

    // Default to true if not specified for backwards compatibility
    const isSendToModel = typeof value === "object" && "SendToModel" in value ? (value.SendToModel ?? true) : true;
    const sendToModelLabel = isSendToModel ? "Sent to model" : "Internal only";

    const displayValue = aiAgentParametersUtils.formatParameterValueForDisplay(extractedValue);

    return (
        <div className="w-100">
            <div className="flex-grow-1 min-width-0 hstack gap-2 mb-1">
                <div className="font-monospace text-truncate flex-grow-1" title={name}>
                    {name}
                </div>
                <Badge bg={isSendToModel ? "success" : "secondary"} className="font-size-12" pill>
                    {sendToModelLabel}
                </Badge>
            </div>
            <Form.Control type="text" value={displayValue} disabled className="text-truncate" title={displayValue} />
            {!isLast && <hr className="my-2" />}
        </div>
    );
}
