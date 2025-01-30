import React from "react";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { SelectIndexErrorsDropdown } from "components/pages/database/indexes/errors/SelectIndexErrorsDropdown";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { nameAndCount } from "components/pages/database/indexes/errors/types";
import { ColumnFiltersState, Updater } from "@tanstack/react-table";
import { ErrorInfoItem } from "components/pages/database/indexes/errors/hooks/useIndexErrors";

interface FilterIndexErrorProps {
    erroredIndexNames: nameAndCount[];
    erroredActionNames: nameAndCount[];
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
            <FlexGrow />
            <SelectIndexErrorsDropdown
                isLoading={isLoading}
                filters={filters}
                indexesList={erroredIndexNames}
                setFilters={setFilters}
                dropdownType="IndexName"
            />
            <SelectIndexErrorsDropdown
                isLoading={isLoading}
                filters={filters}
                indexesList={erroredActionNames}
                setFilters={setFilters}
                dropdownType="Action"
            />
            <ButtonWithSpinner onClick={refresh} isSpinning={isLoading} color="primary">
                <Icon icon="refresh" /> Refresh
            </ButtonWithSpinner>
        </div>
    );
}
