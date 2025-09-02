import { CustomDropdownToggle } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import Dropdown from "react-bootstrap/Dropdown";
import Form from "react-bootstrap/Form";

interface AiAgentParametersDropdownProps {
    parameters: Record<string, string>;
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
                    <div key={x.name} className="w-100">
                        <div className="hstack justify-content-between">
                            <Badge
                                bg="primary"
                                className="text-truncate me-2 fs-5"
                                title={x.name}
                                style={{ width: "150px" }}
                                pill
                            >
                                {x.name}
                            </Badge>
                            <div className="flex-grow-1">
                                <Form.Control
                                    type="text"
                                    value={x.value}
                                    disabled
                                    className="text-truncate"
                                    title={x.value}
                                />
                            </div>
                        </div>
                        {idx !== parametersArray.length - 1 && <hr className="my-1" />}
                    </div>
                ))}
            </Dropdown.Menu>
        </Dropdown>
    );
}
