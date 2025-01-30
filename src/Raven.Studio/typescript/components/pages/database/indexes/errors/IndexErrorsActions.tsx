import React from "react";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { NameAndCount } from "components/pages/database/indexes/errors/types";
import { ColumnFiltersState, Updater } from "@tanstack/react-table";
import { ErrorInfoItem } from "components/pages/database/indexes/errors/hooks/useIndexErrors";
import { SelectActionDropdown } from "components/pages/database/indexes/errors/dropdowns/SelectActionDropdown";
import { SelectIndexNameDropdown } from "components/pages/database/indexes/errors/dropdowns/SelectIndexNameDropdown";

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
            <SelectIndexNameDropdown
                isLoading={isLoading}
                filters={filters}
                indexesList={erroredIndexNames}
                setFilters={setFilters}
            />
            <SelectActionDropdown
                isLoading={isLoading}
                filters={filters}
                indexesList={erroredActionNames}
                setFilters={setFilters}
            />
            <ButtonWithSpinner onClick={refresh} icon="refresh" isSpinning={isLoading} color="primary">
                Refresh
            </ButtonWithSpinner>
        </div>
    );
}
