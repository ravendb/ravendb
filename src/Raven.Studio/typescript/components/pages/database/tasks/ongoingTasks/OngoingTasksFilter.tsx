import { Icon } from "components/common/Icon";
import { MultiCheckboxToggle } from "components/common/MultiCheckboxToggle";
import { InputItem } from "components/models/common";
import produce from "immer";
import React from "react";
import { Input, Button } from "reactstrap";

export type OngoingTaskFilterType = "Replication" | "ETL" | "Sink" | "Backup" | "Subscription";

export interface OngoingTasksFilterCriteria {
    searchText: string;
    types: OngoingTaskFilterType[];
}

interface OngoingTasksFilterProps {
    filter: OngoingTasksFilterCriteria;
    setFilter: (x: OngoingTasksFilterCriteria) => void;
    filterByStatusOptions: InputItem<OngoingTaskFilterType>[];
    tasksCount: number;
}

export default function OngoingTasksFilter(props: OngoingTasksFilterProps) {
    const { filter, setFilter, filterByStatusOptions, tasksCount } = props;

    const onSearchTextChange = (searchText: string) => {
        setFilter(
            produce(filter, (draft) => {
                draft.searchText = searchText;
            })
        );
    };

    const onSearchStatusesChange = (types: OngoingTaskFilterType[]) => {
        setFilter(
            produce(filter, (draft) => {
                draft.types = types;
            })
        );
    };

    return (
        <div className="d-flex flex-wrap flex-grow align-items-end gap-3 mb-3">
            <div className="flex-grow">
                <div className="small-label ms-1 mb-1">Filter by name</div>
                <div className="clearable-input">
                    <Input
                        type="text"
                        accessKey="/"
                        placeholder="e.g. MyPeriodicBackupTask"
                        title="Filter ongoing tasks"
                        className="filtering-input"
                        value={filter.searchText}
                        onChange={(e) => onSearchTextChange(e.target.value)}
                    />
                    {filter.searchText && (
                        <div className="clear-button">
                            <Button color="secondary" size="sm" onClick={() => onSearchTextChange("")}>
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            <div>
                <MultiCheckboxToggle
                    inputItems={filterByStatusOptions}
                    label="Filter by type"
                    selectedItems={filter.types}
                    setSelectedItems={onSearchStatusesChange}
                    selectAll
                    selectAllLabel="All"
                    selectAllCount={tasksCount}
                />
            </div>
        </div>
    );
}
