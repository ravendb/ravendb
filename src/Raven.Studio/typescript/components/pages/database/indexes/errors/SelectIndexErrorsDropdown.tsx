import React, { useCallback, useEffect, useMemo, useState } from "react";
import "./SelectIndexDropdownToggle.scss";
import { Badge, DropdownMenu, DropdownToggle, Label, UncontrolledDropdown } from "reactstrap";
import { Checkbox } from "components/common/Checkbox";
import { FlexGrow } from "components/common/FlexGrow";
import { ColumnFilter, ColumnFiltersState, Updater } from "@tanstack/react-table";
import { nameAndCount } from "components/pages/database/indexes/errors/types";

interface SelectIndexErrorsDropdownProps {
    dropdownType: "IndexName" | "Action";
    indexesList: nameAndCount[];
    filters: ColumnFiltersState;
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
    isLoading?: boolean;
}

export function SelectIndexErrorsDropdown(props: SelectIndexErrorsDropdownProps) {
    const { labelText, isAllSelected, isSomeSelected, isFilterSelected, toggleFilter, toggleAllFilters, filteredList } =
        useSelectIndexErrorsDropdown(props);

    return (
        <UncontrolledDropdown className="select-index-errors-dropdown">
            <DropdownToggle
                disabled={props.isLoading}
                className="select-index-errors-toggle d-flex align-items-center"
                caret
            >
                <div className="flex-grow d-flex align-items-center">{labelText}</div>
            </DropdownToggle>
            <DropdownMenu className="p-3 custom-dropdown-menu">
                <div className="vstack gap-2">
                    <div className="hstack lh-1 gap-3">
                        <Checkbox
                            indeterminate={isSomeSelected}
                            selected={isAllSelected}
                            toggleSelection={toggleAllFilters}
                            color="primary"
                        />
                        <Label className="m-0">
                            <small>Select all</small>
                        </Label>
                    </div>
                    <hr className="m-0" />
                    {filteredList.map((item, index) => (
                        <div className="hstack gap-2" key={index}>
                            <div className="hstack lh-1 gap-3 dropdown-checkbox-group">
                                <Checkbox
                                    selected={isFilterSelected(item.name)}
                                    toggleSelection={() => toggleFilter(item.name)}
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

const useSelectIndexErrorsDropdown = ({
    filters,
    dropdownType,
    indexesList,
    setFilters,
}: SelectIndexErrorsDropdownProps) => {
    const filteredList =
        dropdownType === "Action"
            ? indexesList.filter((item) => item.name)
            : indexesList.filter((item) => item.count !== undefined);

    const firstErrorItem = filteredList.find((item) => item.count && item.count > 0);

    const isFilterSelected = useCallback(
        (name: string) => filters.some((filter) => (filter.value as string[]).includes(name)),
        [filters]
    );

    const toggleFilter = (name: string) => {
        setFilters((prev) => {
            const updatedFilters = [...prev];
            const existingFilterIndex = updatedFilters.findIndex((filter) => filter.id === dropdownType);

            if (existingFilterIndex !== -1) {
                const existingFilter = updatedFilters[existingFilterIndex];
                const values = Array.isArray(existingFilter.value) ? existingFilter.value : [];

                if (values.includes(name)) {
                    const newValues = values.filter((value) => value !== name);

                    if (newValues.length > 0) {
                        updatedFilters[existingFilterIndex] = { ...existingFilter, value: newValues };
                    } else {
                        updatedFilters.splice(existingFilterIndex, 1); // Remove filter if no values remain
                    }
                } else {
                    updatedFilters[existingFilterIndex] = { ...existingFilter, value: [...values, name] };
                }
            } else {
                updatedFilters.push({ id: dropdownType, value: [name] });
            }

            return updatedFilters;
        });
    };

    const isAllSelected = useMemo(() => {
        const filter = filters.find((filter) => filter.id === dropdownType) as ColumnFilter & { value: string[] };

        if (!filter) {
            return true;
        }

        return filteredList.every((item) => filter.value.includes(item.name));
    }, [filters, dropdownType, filteredList]);

    const isSomeSelected = useMemo(() => {
        const filter = filters.find((filter) => filter.id === dropdownType) as ColumnFilter & { value: string[] };

        if (!filter) {
            return false;
        }

        const selectedItems = filter.value;

        return selectedItems.length > 0 && selectedItems.length < filteredList.length;
    }, [filters, dropdownType, filteredList]);

    const toggleAllFilters = () => {
        if (isAllSelected || isSomeSelected) {
            setFilters((prevFilters) => {
                const updatedFilters = prevFilters.map((filter) =>
                    filter.id === dropdownType ? { ...filter, id: dropdownType, value: [] } : filter
                );

                const filterExists = updatedFilters.some((filter) => filter.id === dropdownType);

                return filterExists ? updatedFilters : [...updatedFilters, { id: dropdownType, value: [] }];
            });
        } else {
            setFilters((prevFilters) => {
                const updatedFilters = prevFilters.map((filter) =>
                    filter.id === dropdownType ? { ...filter, value: filteredList.map((x) => x.name) } : filter
                );

                const filterExists = updatedFilters.some((filter) => filter.id === dropdownType);

                return filterExists
                    ? updatedFilters
                    : [...updatedFilters, { id: dropdownType, value: filteredList.map((x) => x.name) }];
            });
        }
    };

    const [labelText, setLabelText] = useState<string>(() => {
        switch (dropdownType) {
            case "Action":
                // eslint-disable-next-line no-case-declarations
                const actionFilter = filters.find((filter) => filter.id === "Action");

                if (actionFilter) {
                    const selectedActionFilters = (actionFilter.value as string[]) ?? [];

                    if (selectedActionFilters.length === 0) {
                        return `None actions are selected`;
                    }

                    if (selectedActionFilters.length > 0 && selectedActionFilters.length < filteredList.length) {
                        return `Selected actions (${selectedActionFilters.length})`;
                    }
                    return `All actions selected (${filteredList.length})`;
                }

                return firstErrorItem?.name ?? "Select all";
            case "IndexName":
                // eslint-disable-next-line no-case-declarations
                const indexFilter = filters.find((filter) => filter.id === "IndexName");

                if (indexFilter) {
                    const selectedIndexes = (indexFilter.value as string[]) ?? [];

                    if (selectedIndexes.length === 0) {
                        return `None indexes are selected (0)`;
                    }

                    if (selectedIndexes.length > 0 && selectedIndexes.length < filteredList.length) {
                        return `Selected indexes (${selectedIndexes.length})`;
                    }

                    return `All indexes selected (${filteredList.length})`;
                }

                return `All indexes selected (${filteredList.length})`;
            default:
                return firstErrorItem?.name ?? "Select all";
        }
    });

    useEffect(() => {
        setLabelText(() => {
            switch (dropdownType) {
                case "Action":
                    // eslint-disable-next-line no-case-declarations
                    const actionFilter = filters.find((filter) => filter.id === "Action");

                    if (actionFilter) {
                        const selectedActionFilters = (actionFilter.value as string[]) ?? [];

                        if (selectedActionFilters.length === 0) {
                            return `None actions are selected (0)`;
                        }

                        if (selectedActionFilters.length > 0 && selectedActionFilters.length < filteredList.length) {
                            return `Selected actions (${selectedActionFilters.length})`;
                        }

                        return `All actions selected (${selectedActionFilters.length})`;
                    }

                    return "All actions selected";
                case "IndexName":
                    // eslint-disable-next-line no-case-declarations
                    const indexFilter = filters.find((filter) => filter.id === "IndexName");

                    if (indexFilter) {
                        const selectedIndexesFilters = (indexFilter.value as string[]) ?? [];

                        if (selectedIndexesFilters.length === 0) {
                            return `None indexes are selected (0)`;
                        }

                        if (selectedIndexesFilters.length > 0 && selectedIndexesFilters.length < filteredList.length) {
                            return `Selected indexes (${selectedIndexesFilters.length})`;
                        }

                        return `All indexes selected (${filteredList.length})`;
                    }

                    return `All indexes selected`;
                default:
                    return firstErrorItem?.name ?? "Select all";
            }
        });
    }, [filters, dropdownType]);

    return {
        filteredList,
        labelText,
        isAllSelected,
        isSomeSelected,
        toggleFilter,
        toggleAllFilters,
        isFilterSelected,
    };
};
