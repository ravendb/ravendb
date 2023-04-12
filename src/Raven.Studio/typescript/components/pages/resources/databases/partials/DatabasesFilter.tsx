import React, { ChangeEvent } from "react";
import { DatabaseFilterByStateOption, DatabaseFilterCriteria } from "components/models/databases";
import { Input } from "reactstrap";
import { useAppSelector } from "components/store";

import { MultiCheckboxToggle } from "components/common/MultiCheckboxToggle";
import "./DatabasesFilter.scss";
import { InputItem } from "components/models/common";
import { selectAllDatabasesCount, selectFilterByStateOptions } from "components/common/shell/databaseSliceSelectors";

type FilterByStateOptions = InputItem<DatabaseFilterByStateOption>[];

interface DatabasesFilterProps {
    searchCriteria: DatabaseFilterCriteria;
    setFilterCriteria: (criteria: DatabaseFilterCriteria) => void;
}

export function DatabasesFilter(props: DatabasesFilterProps) {
    const { searchCriteria, setFilterCriteria } = props;

    const allDatabasesCount = useAppSelector(selectAllDatabasesCount);
    const filterByStateOptions: FilterByStateOptions = useAppSelector(selectFilterByStateOptions);

    const onSearchNameChange = (e: ChangeEvent<HTMLInputElement>) => {
        setFilterCriteria({
            name: e.target.value,
            states: searchCriteria.states,
        });
    };

    const onSearchStatusesChange = (states: DatabaseFilterByStateOption[]) => {
        setFilterCriteria({
            name: searchCriteria.name,
            states,
        });
    };

    return (
        <div className="d-flex flex-wrap gap-3 mb-3">
            <div className="d-flex flex-column flex-grow">
                <div className="small-label ms-1 mb-1">Filter by name</div>
                <Input
                    type="text"
                    accessKey="/"
                    placeholder="e.g. database1"
                    title="Filter databases (Alt+/)"
                    value={searchCriteria.name}
                    onChange={onSearchNameChange}
                    className="filtering-input"
                />
            </div>
            <div>
                <MultiCheckboxToggle<DatabaseFilterByStateOption>
                    inputItems={filterByStateOptions}
                    label="Filter by state"
                    selectedItems={searchCriteria.states}
                    setSelectedItems={onSearchStatusesChange}
                    selectAll
                    selectAllLabel="All"
                    selectAllCount={allDatabasesCount}
                />
            </div>
        </div>
    );
}
