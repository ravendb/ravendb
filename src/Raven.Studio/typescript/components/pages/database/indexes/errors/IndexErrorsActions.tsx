import React from "react";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { NameAndCount } from "components/pages/database/indexes/errors/types";
import { ColumnFiltersState, Updater } from "@tanstack/react-table";
import { ErrorInfoItem } from "components/pages/database/indexes/errors/hooks/useIndexErrors";
import { SelectIndexErrorsDropdown } from "components/pages/database/indexes/errors/SelectIndexErrorsDropdown";

interface FilterIndexErrorProps {
    erroredIndexNames: NameAndCount[];
    erroredActionNames: NameAndCount[];
    refresh: () => Promise<Awaited<ErrorInfoItem>[]>;
    isLoading: boolean;
    filters: ColumnFiltersState;
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
}

export default function IndexErrorsActions({
    erroredIndexNames,
    filters,
    setFilters,
    erroredActionNames,
    refresh,
    isLoading,
}: FilterIndexErrorProps) {
    return (
        <div className="hstack flex-wrap align-items-end gap-2 mb-3 justify-content-end">
            <SelectIndexErrorsDropdown
                indexesList={erroredIndexNames}
                isLoading={isLoading}
                filters={filters}
                setFilters={setFilters}
                dropdownTypeLabelText="indexes"
                dropdownType="IndexName"
            />
            <SelectIndexErrorsDropdown
                indexesList={erroredActionNames}
                isLoading={isLoading}
                filters={filters}
                setFilters={setFilters}
                dropdownTypeLabelText="actions"
                dropdownType="Action"
            />
            <ButtonWithSpinner onClick={refresh} icon="refresh" isSpinning={isLoading} color="primary">
                Refresh
            </ButtonWithSpinner>
        </div>
    );
}
