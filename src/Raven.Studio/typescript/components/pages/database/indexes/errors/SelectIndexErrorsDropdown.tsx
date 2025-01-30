import React, { useMemo } from "react";
import "./SelectIndexDropdownToggle.scss";
import { Badge, DropdownMenu, DropdownToggle, Label, UncontrolledDropdown } from "reactstrap";
import { Checkbox } from "components/common/Checkbox";
import { FlexGrow } from "components/common/FlexGrow";
import { NameAndCount } from "components/pages/database/indexes/errors/types";
import CheckboxSelectAll from "components/common/CheckboxSelectAll";
import { useCheckboxes } from "hooks/useCheckboxes";
import { ColumnFiltersState, Updater } from "@tanstack/react-table";

interface SelectIndexErrorsDropdownProps {
    indexesList: NameAndCount[];
    selectedIndexes: string[];
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
    isLoading: boolean;
    dropdownType: "Action" | "IndexName";
}

export function SelectIndexErrorsDropdown({
    indexesList,
    selectedIndexes,
    isLoading,
    setFilters,
    dropdownType,
}: SelectIndexErrorsDropdownProps) {
    const setSelectedIndexes = (selected: string[]) => {
        setFilters((prev) => {
            const updatedFilters = prev.filter((filter) => filter.id !== dropdownType);
            if (selected.length > 0) {
                updatedFilters.push({ id: dropdownType, value: selected });
            }
            return updatedFilters;
        });
    };

    const labelText = useMemo(() => {
        const dropdownTypeLabelText = dropdownType === "Action" ? "actions" : "indexes";
        if (selectedIndexes.length !== 0 && selectedIndexes.length < indexesList.length) {
            return `Selected ${dropdownTypeLabelText} (${selectedIndexes.length})`;
        }

        return `All ${dropdownTypeLabelText} selected`;
    }, [selectedIndexes, indexesList.length]);

    const { selectionState, toggleOne, toggleAll } = useCheckboxes({
        allItems: indexesList.map((item) => item.name),
        selectedItems: selectedIndexes,
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
                            selectedItemsCount={selectedIndexes.length}
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
                                    selected={selectedIndexes.includes(item.name)}
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
