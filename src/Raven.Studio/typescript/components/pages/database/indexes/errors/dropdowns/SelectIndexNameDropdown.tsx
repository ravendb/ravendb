import React from "react";
import { ColumnFiltersState, Updater } from "@tanstack/react-table";
import { NameAndCount } from "components/pages/database/indexes/errors/types";
import { SelectIndexErrorsDropdown } from "components/pages/database/indexes/errors/SelectIndexErrorsDropdown";

interface SelectIndexNameDropdownProps {
    indexesList: NameAndCount[];
    filters: ColumnFiltersState;
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
    isLoading?: boolean;
}

export function SelectIndexNameDropdown({ indexesList, filters, setFilters, isLoading }: SelectIndexNameDropdownProps) {
    const actionFilter = filters.find((filter) => filter.id === "IndexName");
    const selectedIndexes = (actionFilter?.value as string[]) ?? [];

    return (
        <SelectIndexErrorsDropdown
            indexesList={indexesList}
            selectedIndexes={selectedIndexes}
            setFilters={setFilters}
            dropdownType="IndexName"
            isLoading={isLoading}
        />
    );
}
