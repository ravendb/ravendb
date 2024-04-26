import React from "react";
import "./SelectIndexDropdownToggle.scss";
import { UncontrolledDropdown, DropdownToggle, DropdownMenu, Label, Badge } from "reactstrap";
import { Checkbox } from "components/common/Checkbox";
import { FlexGrow } from "components/common/FlexGrow";

interface SelectIndexErrorsDropdownProps {
    indexesList: { name: string; errorCount?: number; erroredAction?: string }[];
    dropdownType: "indexNames" | "erroredActions";
}

export function SelectIndexErrorsDropdown(props: SelectIndexErrorsDropdownProps) {
    const { indexesList, dropdownType } = props;
    const filteredList =
        dropdownType === "erroredActions"
            ? indexesList.filter((item) => item.erroredAction)
            : indexesList.filter((item) => item.errorCount !== undefined);
    const firstErrorItem = filteredList.find((item) => item.errorCount && item.errorCount > 0);
    const labelText =
        dropdownType === "indexNames"
            ? `All indexes selected (${filteredList.length})`
            : firstErrorItem?.erroredAction || "Select all";
    return (
        <UncontrolledDropdown className="select-index-errors-dropdown">
            <DropdownToggle className="select-index-errors-toggle d-flex align-items-center" caret>
                <div className="flex-grow d-flex align-items-center">{labelText}</div>
            </DropdownToggle>
            <DropdownMenu className="p-3 custom-dropdown-menu">
                <div className="vstack gap-2">
                    <div className="hstack lh-1 gap-3">
                        <Checkbox selected={true} toggleSelection={null} color="primary" />{" "}
                        <Label className="m-0">
                            <small>Select all</small>
                        </Label>
                    </div>
                    <hr className="m-0" />
                    {filteredList.map((item, index) => (
                        <div className="hstack gap-2" key={index}>
                            <div className="hstack lh-1 gap-3 dropdown-checkbox-group">
                                <Checkbox selected={null} toggleSelection={null} />
                                <Label className="m-0">
                                    <small>{dropdownType === "erroredActions" ? item.erroredAction : item.name}</small>
                                </Label>
                            </div>
                            <FlexGrow />
                            <Badge color={item.errorCount > 0 ? "faded-danger" : "faded-dark"}>{item.errorCount}</Badge>
                        </div>
                    ))}
                </div>
            </DropdownMenu>
        </UncontrolledDropdown>
    );
}
