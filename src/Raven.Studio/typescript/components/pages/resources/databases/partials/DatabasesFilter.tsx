import React, { ChangeEvent } from "react";
import { DatabaseFilterByStateOption, DatabaseFilterCriteria } from "components/models/databases";
import { Input } from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";

import { MultiCheckboxToggle } from "components/common/MultiCheckboxToggle";
import "./DatabasesFilter.scss";
import { shallowEqual } from "react-redux";
import { InputItem } from "components/models/common";
import {
    selectAllDatabasesCount,
    selectDatabaseSearchCriteria,
    selectFilterByStateOptions,
} from "components/common/shell/databaseSliceSelectors";
import { setSearchCriteriaName, setSearchCriteriaStates } from "components/common/shell/databaseSliceActions";

type FilterByStateOptions = InputItem<DatabaseFilterByStateOption>[];

export function DatabasesFilter() {
    const dispatch = useAppDispatch();

    const allDatabasesCount = useAppSelector(selectAllDatabasesCount);
    const searchCriteria: DatabaseFilterCriteria = useAppSelector(selectDatabaseSearchCriteria, shallowEqual);
    const filterByStateOptions: FilterByStateOptions = useAppSelector(selectFilterByStateOptions);

    const onSearchNameChange = (e: ChangeEvent<HTMLInputElement>) => {
        dispatch(setSearchCriteriaName(e.target.value));
    };

    const selectAllItem: InputItem = {
        label: "All",
        value: "All",
        count: allDatabasesCount,
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
                    setSelectedItems={(x) => dispatch(setSearchCriteriaStates(x))}
                    itemSelectAll={selectAllItem}
                />
            </div>
        </div>
    );
}
