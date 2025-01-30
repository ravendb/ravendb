import React from "react";
import { ColumnFiltersState, Updater } from "@tanstack/react-table";
import { NameAndCount } from "components/pages/database/indexes/errors/types";
import { SelectIndexErrorsDropdown } from "components/pages/database/indexes/errors/SelectIndexErrorsDropdown";

interface SelectActionDropdownProps {
    indexesList: NameAndCount[];
    filters: ColumnFiltersState;
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
    isLoading?: boolean;
}

export function SelectActionDropdown({ indexesList, filters, setFilters, isLoading }: SelectActionDropdownProps) {
    const actionFilter = filters.find((filter) => filter.id === "Action");
    const selectedIndexes = (actionFilter?.value as string[]) ?? [];

    return (
        <SelectIndexErrorsDropdown
            indexesList={indexesList}
            selectedIndexes={selectedIndexes}
            setFilters={setFilters}
            dropdownType="Action"
            isLoading={isLoading}
        />
    );
}
