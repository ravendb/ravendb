import React from "react";
import "./SelectIndexDropdownToggle.scss";
import { Badge, DropdownMenu, DropdownToggle, Label, UncontrolledDropdown } from "reactstrap";
import { Checkbox } from "components/common/Checkbox";
import { FlexGrow } from "components/common/FlexGrow";
import { IndexErrorsDropdownType, NameAndCount } from "components/pages/database/indexes/errors/types";
import CheckboxSelectAll from "components/common/CheckboxSelectAll";
import { useCheckboxes } from "hooks/useCheckboxes";
import { useIndexErrorsDropdown } from "components/pages/database/indexes/errors/hooks/useIndexErrorsDropdown";
import { ColumnFiltersState, Updater } from "@tanstack/react-table";

interface SelectIndexErrorsDropdownProps {
    indexesList: NameAndCount[];
    filters: ColumnFiltersState;
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
    isLoading: boolean;
    dropdownTypeLabelText: string;
    dropdownType: IndexErrorsDropdownType;
}

export function SelectIndexErrorsDropdown({
    indexesList,
    dropdownType,
    setFilters,
    filters,
    dropdownTypeLabelText,
    isLoading,
}: SelectIndexErrorsDropdownProps) {
    const { setSelectedIndexes, labelText, selectedColumnFilters } = useIndexErrorsDropdown({
        indexesList,
        filters,
        dropdownType,
        setFilters,
        dropdownTypeLabelText,
    });

    const { selectionState, toggleOne, toggleAll } = useCheckboxes({
        allItems: indexesList.map((item) => item.name),
        selectedItems: selectedColumnFilters,
        setValue: setSelectedIndexes,
    });

    return (
        <UncontrolledDropdown className="select-index-errors-dropdown">
            <DropdownToggle disabled={isLoading} className="select-index-errors-toggle d-flex align-items-center" caret>
                <div className="flex-grow d-flex align-items-center">{labelText}</div>
            </DropdownToggle>
            <DropdownMenu className="p-3 custom-dropdown-menu">
                <div className="vstack gap-2">
                    <div className="hstack lh-1 gap-3">
                        <CheckboxSelectAll
                            selectedItemsCount={selectedColumnFilters.length}
                            allItemsCount={indexesList.length}
                            color="primary"
                            selectionState={selectionState}
                            toggleAll={toggleAll}
                        />
                    </div>
                    <hr className="m-0" />
                    {indexesList.map((item, index) => (
                        <div className="hstack gap-2" key={index}>
                            <div className="hstack lh-1 gap-3 dropdown-checkbox-group">
                                <Checkbox
                                    selected={selectedColumnFilters.includes(item.name)}
                                    toggleSelection={() => toggleOne(item.name)}
                                />
                                <Label className="m-0">
                                    <small>{item.name}</small>
                                </Label>
                            </div>
                            <FlexGrow />
                            <Badge color={item.count > 0 ? "faded-danger" : "faded-dark"}>{item.count}</Badge>
                        </div>
                    ))}
                </div>
            </DropdownMenu>
        </UncontrolledDropdown>
    );
}
