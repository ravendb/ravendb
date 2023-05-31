import React, { ChangeEvent, useCallback, useState } from "react";
import classNames from "classnames";
import { shardingTodo } from "common/developmentHelper";
import { IndexStatus, IndexFilterCriteria, IndexSharedInfo } from "components/models/indexes";
import { Button, DropdownItem, Input, InputGroup } from "reactstrap";
import useBoolean from "hooks/useBoolean";
import { DropdownPanel } from "components/common/DropdownPanel";
import { Switch } from "components/common/Checkbox";
import produce from "immer";
import { Icon } from "components/common/Icon";

interface IndexFilterStatusItemProps {
    label: string;
    color?: string;
    toggleClass?: string;
    toggleStatus: () => void;
    checked: boolean;
    children?: any;
}

function IndexFilterStatusItem(props: IndexFilterStatusItemProps) {
    return (
        <React.Fragment>
            <div className="dropdown-item-text">
                <Switch
                    color={props.color}
                    className={props.toggleClass}
                    selected={props.checked}
                    toggleSelection={props.toggleStatus}
                    reverse
                >
                    {props.label}
                </Switch>
                {props.children}
            </div>
        </React.Fragment>
    );
}

interface IndexFilterProps {
    filter: IndexFilterCriteria;
    setFilter: React.Dispatch<React.SetStateAction<IndexFilterCriteria>>;
}

function hasAnyStateFilter(filter: IndexFilterCriteria) {
    const autoRefresh = filter.autoRefresh;
    const filterCount = filter.status;
    const withIndexingErrorsOnly = filter.showOnlyIndexesWithIndexingErrors;

    return !autoRefresh || filterCount.length !== 7 || withIndexingErrorsOnly;
}

interface IndexFilterDescriptionProps {
    filter: IndexFilterCriteria;
    setFilter: (x: IndexFilterCriteria) => void;
    indexes: IndexSharedInfo[];
}

export function IndexFilterDescription(props: IndexFilterDescriptionProps) {
    const { filter, setFilter } = props;

    //TODO: const indexesCount = indexes.length;

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

    if (!filter.status.length) {
        return (
            <div className="on-base-background mt-2">
                All <strong>Index Status</strong> options are unchecked. Please select options under{" "}
                <strong>&apos;Index Status&apos;</strong> to view indexes list.
            </div>
        );
    }

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
                {/* TODO: add selectedItems and setSelectedItems */}
                {/* <MultiCheckboxToggle
                    inputItems={indexesStatesList}
                    label="Filter by state"
                    selectedItems={[]}
                    setSelectedItems={() => {
                        // TODO: add logic
                    }}
                /> */}
            </div>
            {/* TODO: `Processing Speed: <strong>${Math.floor(totalProcessedPerSecond).toLocaleString()}</strong> docs / sec`;*/}
            {/* TODO: add auto refresh (is it needed) */}
            {/* <Switch id="autoRefresh" toggleSelection={null} selected={null} color="info" className="mt-1">
                <span>Auto refresh is {filter.autoRefresh ? "on" : "off"}</span>
            </Switch>
            <UncontrolledPopover target="autoRefresh" trigger="hover" placement="bottom">
                <PopoverBody>
                    Automatically refreshes the list of indexes.
                    <br />
                    Might result in list flickering.
                </PopoverBody>
            </UncontrolledPopover> */}
        </div>
    );
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const indexesStatesList = [
    { value: "all", label: "All", count: 8 },
    { value: "normal", label: "Normal" },
    { value: "error", label: "Error/Faulty" },
    { value: "stale", label: "Stale" },
    { value: "rollingdeployment", label: "Rolling deployment" },
    { value: "paused", label: "Paused" },
    { value: "disabled", label: "Disabled" },
    { value: "idle", label: "Idle" },
];

// TODO: remove this component after the MultiToggle in IndexFilterDescription is properly connected
export default function IndexFilter(props: IndexFilterProps) {
    const { filter, setFilter } = props;

    const toggleStatus = useCallback(
        (status: IndexStatus) => {
            console.log("status toggled " + status);
            setFilter((f) => ({
                ...f,
                status: filter.status.includes(status)
                    ? filter.status.filter((x) => x !== status)
                    : filter.status.concat(status),
            }));
        },
        [filter, setFilter]
    );

    const onSearchTextChange = (e: ChangeEvent<HTMLInputElement>) => {
        props.setFilter((f) => ({
            ...f,
            searchText: e.target.value,
        }));
    };

    const toggleIndexesWithErrors = () => {
        props.setFilter((f) => ({
            ...f,
            showOnlyIndexesWithIndexingErrors: !f.showOnlyIndexesWithIndexingErrors,
        }));
    };

    const toggleAutoRefresh = () => {
        props.setFilter((f) => ({
            ...f,
            autoRefresh: !f.autoRefresh,
        }));
    };

    const [filterReferenceElement, setFilterReferenceElement] = useState(null);
    const { value: filterDropdownVisible, toggle: toggleFilterDropdown } = useBoolean(false);

    return (
        <InputGroup data-label="Filter" className="d-none">
            {/*TODO: Remove this component after the MultiToggle is properly connected */}
            <Input
                type="text"
                accessKey="/"
                placeholder="Index Name"
                title="Filter indexes"
                value={filter.searchText}
                onChange={onSearchTextChange}
            />
            <Button
                innerRef={setFilterReferenceElement}
                onClick={toggleFilterDropdown}
                color={hasAnyStateFilter(filter) ? "light" : "secondary"}
                title="Set the indexing state for the selected indexes"
                className={classNames("dropdown-toggle")}
            >
                <span>Index Status</span>
            </Button>
            <DropdownPanel
                visible={filterDropdownVisible}
                toggle={toggleFilterDropdown}
                buttonRef={filterReferenceElement}
            >
                <IndexFilterStatusItem
                    toggleStatus={() => toggleStatus("Normal")}
                    checked={filter.status.includes("Normal")}
                    label="Normal"
                    color="success"
                />
                <IndexFilterStatusItem
                    toggleStatus={() => toggleStatus("ErrorOrFaulty")}
                    checked={filter.status.includes("ErrorOrFaulty")}
                    label="Error / Faulty"
                    color="danger"
                />
                <IndexFilterStatusItem
                    toggleStatus={() => toggleStatus("Stale")}
                    checked={filter.status.includes("Stale")}
                    label="Stale"
                    color="warning"
                />
                <IndexFilterStatusItem
                    toggleStatus={() => toggleStatus("RollingDeployment")}
                    checked={filter.status.includes("RollingDeployment")}
                    label="Rolling deployment"
                    color="warning"
                />
                <IndexFilterStatusItem
                    toggleStatus={() => toggleStatus("Paused")}
                    checked={filter.status.includes("Paused")}
                    label="Paused"
                    color="warning"
                />
                <IndexFilterStatusItem
                    toggleStatus={() => toggleStatus("Disabled")}
                    checked={filter.status.includes("Disabled")}
                    label="Disabled"
                    color="warning"
                />
                <IndexFilterStatusItem
                    toggleStatus={() => toggleStatus("Idle")}
                    checked={filter.status.includes("Idle")}
                    label="Idle"
                    color="warning"
                />
                <DropdownItem divider />
                <div className="bg-faded-warning">
                    <IndexFilterStatusItem
                        toggleStatus={toggleIndexesWithErrors}
                        checked={filter.showOnlyIndexesWithIndexingErrors}
                        label="With indexing errors only"
                        color="warning"
                    />
                </div>
                <div className="bg-faded-info">
                    <IndexFilterStatusItem
                        toggleStatus={toggleAutoRefresh}
                        checked={filter.autoRefresh}
                        label="Auto refresh"
                        color="warning"
                    >
                        <div className="fs-5">
                            Automatically refreshes the list of indexes.
                            <br />
                            Might result in list flickering.
                        </div>
                    </IndexFilterStatusItem>
                </div>
            </DropdownPanel>
        </InputGroup>
    );
}
