import React, { ChangeEvent } from "react";
import classNames from "classnames";
import { shardingTodo } from "common/developmentHelper";
import { IndexStatus, IndexFilterCriteria, IndexGroup, IndexSharedInfo } from "../../../../models/indexes";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import IndexUtils from "../../../../utils/IndexUtils";
import { DropdownPanel } from "../../../../common/DropdownPanel";

interface IndexFilterStatusItemProps {
    label: string;
    toggleClass?: string;
    toggleStatus: () => void;
    checked: boolean;
}

function IndexFilterStatusItem(props: IndexFilterStatusItemProps) {
    return (
        <div className="flex-horizontal">
            <div className="control-label flex-grow">{props.label}</div>
            <div className="flex-noshrink">
                <div className={classNames("toggle", props.toggleClass)}>
                    <input type="checkbox" className="styled" checked={props.checked} onChange={props.toggleStatus} />
                    <label />
                </div>
            </div>
        </div>
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
    indexes: IndexSharedInfo[];
}

export function IndexFilterDescription(props: IndexFilterDescriptionProps) {
    const { filter, indexes } = props;

    let indexesCount = indexes.length;

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
            <div>
                <small className="on-base-background">
                    All <strong>Index Status</strong> options are unchecked. Please select options under{" "}
                    <strong>'Index Status'</strong> to view indexes list.
                </small>
            </div>
        );
    }

    const indexingErrorsOnlyPart = filter.showOnlyIndexesWithIndexingErrors ? (
        <>
            , with <strong>indexing errors only</strong>,
        </>
    ) : (
        ""
    );

    const firstPart = indexesCount ? (
        <>
            Displaying <strong>{indexesCount}</strong>{" "}
            {pluralizeHelpers.pluralize(indexesCount, "index", "indexes", true)}
            {indexingErrorsOnlyPart} that match Status Filter:
        </>
    ) : (
        "No matching indexes for Status Filter: "
    );

    return (
        <div>
            <small className="on-base-background">
                {firstPart}
                <strong>{filter.status.map((x) => IndexUtils.formatStatus(x)).join(", ")}</strong>
                {filter.searchText ? (
                    <>
                        , where name contains <strong>{filter.searchText}</strong>
                    </>
                ) : (
                    ""
                )}
                . Auto refresh is <strong>{filter.autoRefresh ? "on" : "off"}</strong>.
                {/* TODO: `Processing Speed: <strong>${Math.floor(totalProcessedPerSecond).toLocaleString()}</strong> docs / sec`;*/}
            </small>
        </div>
    );
}

export default function IndexFilter(props: IndexFilterProps) {
    const { filter } = props;

    const toggleStatus = (status: IndexStatus) => {
        props.setFilter((f) => ({
            ...f,
            status: filter.status.includes(status)
                ? filter.status.filter((x) => x !== status)
                : filter.status.concat(status),
        }));
    };

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

    return (
        <div className="btn-group-label" data-label="Filter">
            <input
                type="text"
                accessKey="/"
                className="form-control"
                placeholder="Index Name"
                title="Filter indexes"
                value={filter.searchText}
                onChange={onSearchTextChange}
            />
            <div className="dropdown dropdown-right">
                <button
                    className={classNames("btn btn-default dropdown-toggle", { active: hasAnyStateFilter(filter) })}
                    type="button"
                    data-toggle="dropdown"
                >
                    <span>Index Status</span>
                    <span className="caret" />
                </button>
                <DropdownPanel className="settings-menu">
                    <div className="settings-item">
                        <div className="margin-left margin-right margin-right-sm">
                            <IndexFilterStatusItem
                                toggleStatus={() => toggleStatus("Normal")}
                                checked={filter.status.includes("Normal")}
                                label="Normal"
                                toggleClass="toggle-success"
                            />
                            <IndexFilterStatusItem
                                toggleStatus={() => toggleStatus("ErrorOrFaulty")}
                                checked={filter.status.includes("ErrorOrFaulty")}
                                label="Error / Faulty"
                                toggleClass="toggle-danger"
                            />
                            <IndexFilterStatusItem
                                toggleStatus={() => toggleStatus("Stale")}
                                checked={filter.status.includes("Stale")}
                                label="Stale"
                                toggleClass="toggle-warning"
                            />
                            <IndexFilterStatusItem
                                toggleStatus={() => toggleStatus("RollingDeployment")}
                                checked={filter.status.includes("RollingDeployment")}
                                label="Rolling deployment"
                                toggleClass="toggle-warning"
                            />
                            <IndexFilterStatusItem
                                toggleStatus={() => toggleStatus("Paused")}
                                checked={filter.status.includes("Paused")}
                                label="Paused"
                                toggleClass="toggle-warning"
                            />
                            <IndexFilterStatusItem
                                toggleStatus={() => toggleStatus("Disabled")}
                                checked={filter.status.includes("Disabled")}
                                label="Disabled"
                                toggleClass="toggle-warning"
                            />
                            <IndexFilterStatusItem
                                toggleStatus={() => toggleStatus("Idle")}
                                checked={filter.status.includes("Idle")}
                                label="Idle"
                                toggleClass="toggle-warning"
                            />
                        </div>
                        <div className="bg-warning auto-update-container">
                            <div className="flex-horizontal">
                                <div className="control-label flex-grow">With indexing errors only</div>
                                <div className="flex-noshrink">
                                    <div className="toggle toggle-danger">
                                        <input
                                            type="checkbox"
                                            className="styled"
                                            checked={filter.showOnlyIndexesWithIndexingErrors}
                                            onChange={toggleIndexesWithErrors}
                                        />
                                        <label />
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="bg-info auto-update-container">
                            <div className="flex-horizontal">
                                <div className="control-label flex-grow">Auto refresh</div>
                                <div className="flex-noshrink">
                                    <div className="toggle toggle-info">
                                        <input
                                            type="checkbox"
                                            className="styled"
                                            checked={filter.autoRefresh}
                                            onChange={toggleAutoRefresh}
                                        />
                                        <label />
                                    </div>
                                </div>
                            </div>
                            <div className="margin-right margin-right-sm">
                                <small>
                                    Automatically refreshes the list of indexes. Might result in list flickering.
                                </small>
                            </div>
                        </div>
                    </div>
                </DropdownPanel>
            </div>
        </div>
    );
}
