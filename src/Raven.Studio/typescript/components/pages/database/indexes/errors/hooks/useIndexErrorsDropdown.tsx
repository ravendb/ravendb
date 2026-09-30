import { ColumnFiltersState, Updater } from "@tanstack/react-table";
import { useMemo } from "react";
import { IndexErrorsDropdownType } from "components/pages/database/indexes/errors/types";

interface UseIndexErrorsDropdownProps {
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
    dropdownType: IndexErrorsDropdownType;
    dropdownTypeLabelText: string;
    filters: ColumnFiltersState;
}

export function useIndexErrorsDropdown({
    setFilters,
    dropdownType,
    filters,
    dropdownTypeLabelText,
}: UseIndexErrorsDropdownProps) {
    const columnFilter = filters.find((filter) => filter.id === dropdownType);
    const selectedColumnFilters = (columnFilter?.value as string[]) ?? [];

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
        if (selectedColumnFilters.length > 0) {
            return `Selected ${dropdownTypeLabelText} (${selectedColumnFilters.length})`;
        }

        return `All ${dropdownTypeLabelText} selected`;
    }, [selectedColumnFilters.length, dropdownTypeLabelText]);

    return {
        labelText,
        setSelectedIndexes,
        selectedColumnFilters,
    };
}
