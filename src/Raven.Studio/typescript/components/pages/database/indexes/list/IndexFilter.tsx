import React, { ChangeEvent } from "react";
import classNames from "classnames";
import { shardingTodo } from "common/developmentHelper";
import { IndexStatus, IndexFilterCriteria, IndexSharedInfo } from "../../../../models/indexes";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import IndexUtils from "../../../../utils/IndexUtils";
import { DropdownPanel } from "../../../../common/DropdownPanel";
import {
    Badge,
    Dropdown,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    FormGroup,
    Input,
    InputGroup,
    Label,
    UncontrolledDropdown,
} from "reactstrap";

interface IndexFilterStatusItemProps {
    label: string;
    color?: string;
    toggleClass?: string;
    toggleStatus: () => void;
    checked: boolean;
    children?: any;
}

function IndexFilterStatusItem(props: IndexFilterStatusItemProps) {
    const switchColor = `form-check-${props.color ?? "secondary"}`;

    return (
        <DropdownItem text>
            <FormGroup switch className={classNames("form-check-reverse", switchColor, props.toggleClass)}>
                <Input type="switch" role="switch" checked={props.checked} onChange={props.toggleStatus} />
                <Label check>{props.label}</Label>
            </FormGroup>
            {props.children}
        </DropdownItem> //TODO: Fix buttons lag
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

    const indexesCount = indexes.length;

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

    const indexingErrorsOnlyPart = filter.showOnlyIndexesWithIndexingErrors ? (
        <>
            <Badge pill color="warning" className="mx-1">
                indexing errors only
            </Badge>{" "}
        </>
    ) : (
        ""
    );

    const firstPart = (
        <>
            <span className="text-capital me-2">
                <strong className="text-emphasis">{indexesCount}</strong>{" "}
                {pluralizeHelpers.pluralize(indexesCount, "index", "indexes", true)}
                {" found "}
            </span>
            {indexingErrorsOnlyPart}
        </>
    );

    return (
        <div className="on-base-background mt-2">
            {firstPart}
            Status filter:
            {filter.status.map((x) => (
                <Badge color="secondary" className="ms-1" pill key={x}>
                    {IndexUtils.formatStatus(x)}
                </Badge>
            ))}
            {filter.searchText ? (
                <span className="ms-2">
                    Name contains: <em className="text-emphasis">&quot;{filter.searchText}&quot;</em>
                </span>
            ) : (
                ""
            )}
            <span className="ms-2">
                Auto refresh is <strong className="text-emphasis">{filter.autoRefresh ? "on" : "off"}</strong>.
            </span>
            {/* TODO: `Processing Speed: <strong>${Math.floor(totalProcessedPerSecond).toLocaleString()}</strong> docs / sec`;*/}
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
        <InputGroup data-label="Filter">
            <Input
                type="text"
                accessKey="/"
                placeholder="Index Name"
                title="Filter indexes"
                value={filter.searchText}
                onChange={onSearchTextChange}
            />
            <UncontrolledDropdown className="mr-1">
                <DropdownToggle
                    key="toggle"
                    outline={hasAnyStateFilter(filter)}
                    title="Set the indexing state for the selected indexes"
                    className={classNames("btn btn-default dropdown-toggle")}
                >
                    <span>Index Status</span>
                </DropdownToggle>

                <DropdownMenu key="menu">
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
                </DropdownMenu>
            </UncontrolledDropdown>
        </InputGroup>
    );
}
