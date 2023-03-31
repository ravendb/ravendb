import React, { ChangeEvent } from "react";
import { DatabaseFilterByStateOption, DatabaseFilterCriteria, DatabaseSharedInfo } from "components/models/databases";
import { Col, Input, Row } from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";
import {
    selectAllDatabases,
    selectDatabaseSearchCriteria,
    selectFilterByStateOptions,
    setSearchCriteriaName,
    setSearchCriteriaStates,
} from "components/common/shell/databasesSlice";
import { MultiCheckboxToggle } from "components/common/MultiCheckboxToggle";
import "./DatabasesFilter.scss";
import { shallowEqual } from "react-redux";
import { InputItem } from "components/models/common";

type FilterByStateOptions = InputItem<DatabaseFilterByStateOption>[];

export function DatabasesFilter() {
    const dispatch = useAppDispatch();

    const allDatabases: DatabaseSharedInfo[] = useAppSelector(selectAllDatabases);
    const searchCriteria: DatabaseFilterCriteria = useAppSelector(selectDatabaseSearchCriteria, shallowEqual);
    const filterByStateOptions: FilterByStateOptions = useAppSelector(selectFilterByStateOptions);

    const onSearchNameChange = (e: ChangeEvent<HTMLInputElement>) => {
        dispatch(setSearchCriteriaName(e.target.value));
    };

    const selectAllItem: InputItem = {
        label: "All",
        value: "All",
        count: allDatabases.length,
    };

    return (
        <Row className="d-flex align-items-end mb-3">
            <Col>
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
            </Col>
            <Col>
                <MultiCheckboxToggle<DatabaseFilterByStateOption>
                    inputItems={filterByStateOptions}
                    label="Filter by state"
                    selectedItems={searchCriteria.states}
                    setSelectedItems={(x) => dispatch(setSearchCriteriaStates(x))}
                    itemSelectAll={selectAllItem}
                />
            </Col>
            <Col></Col>
        </Row>
    );
}
