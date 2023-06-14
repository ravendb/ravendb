import React from "react";
import { shardingTodo } from "common/developmentHelper";
import { IndexStatus, IndexFilterCriteria } from "components/models/indexes";
import { Button, Input, PopoverBody, UncontrolledPopover } from "reactstrap";
import produce from "immer";
import { Icon } from "components/common/Icon";
import { MultiCheckboxToggle } from "components/common/MultiCheckboxToggle";
import { InputItem } from "components/models/common";
import { Switch } from "components/common/Checkbox";

interface IndexFilterProps {
    filter: IndexFilterCriteria;
    setFilter: (x: IndexFilterCriteria) => void;
    filterByStatusOptions: InputItem<IndexStatus>[];
    indexesCount: number;
}

export default function IndexFilter(props: IndexFilterProps) {
    const { filter, setFilter, filterByStatusOptions, indexesCount } = props;

    shardingTodo();

    /* TODO
    let totalProcessedPerSecond = 0;

    this.indexGroups().forEach(indexGroup => {
        const indexesInGroup = indexGroup.indexes().filter(i => !i.filteredOut());
        indexesCount += indexesInGroup.length;

        totalProcessedPerSecond += _.sum(indexesInGroup
            .filter(i => i.progress() || (i.replacement() && i.replacement().progress()))
            .map(i => {
                let sum = 0;

                const progress = i.progress();
                if (progress) {
                    sum += progress.globalProgress().processedPerSecond();
                }

                const replacement = i.replacement();
                if (replacement) {
                    const replacementProgress = replacement.progress();
                    if (replacementProgress) {
                        sum += replacementProgress.globalProgress().processedPerSecond();
                    }
                }

                return sum;
            }));
    });
    */

    /* TODO
    const indexingErrorsOnlyPart = filter.showOnlyIndexesWithIndexingErrors ? (
        <>
            <Badge pill color="warning" className="mx-1">
                indexing errors only
            </Badge>{" "}
        </>
    ) : (
        ""
    );*/

    const onSearchTextChange = (searchText: string) => {
        setFilter(
            produce(filter, (draft) => {
                draft.searchText = searchText;
            })
        );
    };

    const onSearchStatusesChange = (statuses: IndexStatus[]) => {
        setFilter(
            produce(filter, (draft) => {
                draft.statuses = statuses;
            })
        );
    };

    const toggleAutoRefreshSelection = () => {
        setFilter(
            produce(filter, (draft) => {
                draft.autoRefresh = !draft.autoRefresh;
            })
        );
    };

    return (
        <div className="d-flex flex-wrap align-items-end gap-3 mb-3">
            <div className="flex-grow">
                <div className="small-label ms-1 mb-1">Filter by name</div>
                <div className="clearable-input">
                    <Input
                        type="text"
                        accessKey="/"
                        placeholder="e.g. Orders/ByCompany/*"
                        title="Filter indexes"
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
                    label="Filter by state"
                    selectedItems={filter.statuses}
                    setSelectedItems={onSearchStatusesChange}
                    selectAll
                    selectAllLabel="All"
                    selectAllCount={indexesCount}
                />
            </div>
            {/* TODO: `Processing Speed: <strong>${Math.floor(totalProcessedPerSecond).toLocaleString()}</strong> docs / sec`;*/}
            <Switch
                id="autoRefresh"
                toggleSelection={toggleAutoRefreshSelection}
                selected={filter.autoRefresh}
                color="info"
                className="mt-1"
            >
                <span>Auto refresh is {filter.autoRefresh ? "on" : "off"}</span>
            </Switch>
            <UncontrolledPopover target="autoRefresh" trigger="hover" placement="bottom">
                <PopoverBody>
                    Automatically refreshes the list of indexes.
                    <br />
                    Might result in list flickering.
                </PopoverBody>
            </UncontrolledPopover>
        </div>
    );
}
