import React, { useRef, useState } from "react";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import { Icon } from "components/common/Icon";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { FilterChips, FilterMenu, MenuHeader } from "components/common/filterMenu/FilterMenu";
import Select, {
    OptionWithIcon,
    SelectOption,
    SelectOptionWithIcon,
    SingleValueWithIcon,
} from "components/common/select/Select";
import { SortDirection } from "components/models/common";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import { ReplicationHubSinksState, SinkSortField, sinkSortFields } from "./useReplicationHubSinks";
import "./ReplicationHubSinksHeader.scss";

interface ReplicationHubSinksHeaderProps {
    sinksState: ReplicationHubSinksState;
}

const sinksExpandAnimationMs = 500;

function findScrollContainer(element: HTMLElement): HTMLElement {
    for (let current = element.parentElement; current; current = current.parentElement) {
        const { overflowY } = getComputedStyle(current);
        if ((overflowY === "auto" || overflowY === "scroll") && current.scrollHeight > current.clientHeight) {
            return current;
        }
    }
    return (document.scrollingElement as HTMLElement) ?? document.documentElement;
}

function keepTopVisibleSinkInPlace(header: HTMLElement, changeSinksLayout: () => void) {
    const headerBottom = header.getBoundingClientRect().bottom;
    const sinks = Array.from(header.parentElement.querySelectorAll<HTMLElement>(":scope > .rich-panel-item"));
    const topVisibleSink =
        sinks.find((x) => x.getBoundingClientRect().top >= headerBottom) ??
        sinks.find((x) => x.getBoundingClientRect().bottom > headerBottom);
    const initialTop = topVisibleSink?.getBoundingClientRect().top;

    changeSinksLayout();

    if (!topVisibleSink) {
        return;
    }

    const scrollContainer = findScrollContainer(header);
    const endTime = performance.now() + sinksExpandAnimationMs;

    const restorePosition = () => {
        scrollContainer.scrollTop += topVisibleSink.getBoundingClientRect().top - initialTop;
        if (performance.now() < endTime) {
            requestAnimationFrame(restorePosition);
        }
    };
    requestAnimationFrame(restorePosition);
}

export function ReplicationHubSinksHeader({ sinksState }: ReplicationHubSinksHeaderProps) {
    const {
        filterCategories,
        hasActiveFilters,
        totalSinksCount,
        visibleSinks,
        areAllVisibleSinksExpanded,
        toggleAllExpanded,
    } = sinksState;

    const headerRef = useRef<HTMLDivElement>(null);

    return (
        <>
            <div
                ref={headerRef}
                className="replication-hub-sinks-header d-flex align-items-center justify-content-between gap-2"
            >
                <div className="d-flex align-items-center gap-2">
                    <h4 className="mb-0">Replication sinks</h4>
                    <Badge pill bg="secondary">
                        {totalSinksCount}
                    </Badge>
                </div>
                <ButtonGroup size="sm" className="sinks-header-actions">
                    <SortDropdown sinksState={sinksState} />
                    <FilterMenu categories={filterCategories} title="Filter sinks" />
                    <Button
                        variant="secondary"
                        onClick={() => keepTopVisibleSinkInPlace(headerRef.current, toggleAllExpanded)}
                        title={areAllVisibleSinksExpanded ? "Collapse all sinks" : "Expand all sinks"}
                    >
                        <Icon icon={areAllVisibleSinksExpanded ? "fold" : "unfold"} margin="m-0" />
                    </Button>
                </ButtonGroup>
            </div>
            <FilterChips categories={filterCategories} className="replication-hub-sinks-chips">
                <span className="sinks-results-count">
                    <strong>{visibleSinks.length}</strong> of{" "}
                    {pluralizeHelpers.pluralize(totalSinksCount, "sink", "sinks")}
                </span>
            </FilterChips>
            <div className="visually-hidden" role="status">
                {hasActiveFilters
                    ? `${visibleSinks.length} of ${pluralizeHelpers.pluralize(totalSinksCount, "sink", "sinks")} shown`
                    : ""}
            </div>
        </>
    );
}

function getSortFieldInfo(field: SinkSortField) {
    return sinkSortFields.find((x) => x.field === field);
}

const sortMenuPopperConfig = { modifiers: [{ name: "offset", options: { offset: [0, 7] } }] };

const sortFieldOptions: SelectOptionWithIcon<SinkSortField>[] = sinkSortFields.map(({ field, label, icon }) => ({
    value: field,
    label,
    icon,
}));

const sortDirectionOptions: SelectOption<SortDirection>[] = [
    { value: "asc", label: "Ascending" },
    { value: "desc", label: "Descending" },
];

const statusSortDirectionOptions: SelectOption<SortDirection>[] = [
    { value: "asc", label: "Problems first" },
    { value: "desc", label: "Healthy first" },
];

function getSortDirectionOptions(field: SinkSortField) {
    return field === "status" ? statusSortDirectionOptions : sortDirectionOptions;
}

function SortDropdown({ sinksState }: { sinksState: ReplicationHubSinksState }) {
    const { sort } = sinksState;
    const fieldLabel = getSortFieldInfo(sort.field).label;
    const directionLabel = getSortDirectionOptions(sort.field).find((x) => x.value === sort.direction).label;

    return (
        <Dropdown as={ButtonGroup} autoClose="outside" align="end" className="sinks-sort-dropdown">
            <Dropdown.Toggle
                as={CustomDropdownToggle}
                isCaretHidden
                className="sinks-sort-button"
                title={`Sorted by ${fieldLabel}, ${directionLabel.toLowerCase()}`}
            >
                <span className="sinks-sort-button-label">Sort by:</span>
                <span className="sinks-sort-button-value">{fieldLabel}</span>
                <Icon icon={sort.direction === "asc" ? "arrow-thin-top" : "arrow-thin-bottom"} margin="ms-1" />
            </Dropdown.Toggle>
            <Dropdown.Menu className="sinks-sort-menu" popperConfig={sortMenuPopperConfig}>
                <SortMenu sinksState={sinksState} />
            </Dropdown.Menu>
        </Dropdown>
    );
}

function useEscapeClosesOnlyOpenList() {
    const [isListOpen, setIsListOpen] = useState(false);

    return {
        onMenuOpen: () => setIsListOpen(true),
        onMenuClose: () => setIsListOpen(false),
        onKeyDown: (e: React.KeyboardEvent) => {
            if (e.key === "Escape" && isListOpen) {
                e.stopPropagation();
            }
        },
    };
}

function SortMenu({ sinksState }: { sinksState: ReplicationHubSinksState }) {
    const { sort, setSort, isDefaultSort, resetSort } = sinksState;
    const directionOptions = getSortDirectionOptions(sort.field);
    const fieldListEscape = useEscapeClosesOnlyOpenList();
    const directionListEscape = useEscapeClosesOnlyOpenList();

    return (
        <>
            <MenuHeader title="Sort" actionLabel="Reset" isActionVisible={!isDefaultSort} onAction={resetSort} />
            <div className="sinks-sort-selects filter-menu-field">
                <Select<SelectOptionWithIcon<SinkSortField>>
                    aria-label="Sort by"
                    className="sinks-sort-field-select"
                    options={sortFieldOptions}
                    value={sortFieldOptions.find((x) => x.value === sort.field)}
                    onChange={(x) => setSort({ ...sort, field: x.value })}
                    components={{ Option: OptionWithIcon, SingleValue: SingleValueWithIcon }}
                    isSearchable={false}
                    menuPortalTarget={null}
                    {...fieldListEscape}
                />
                <Select<SelectOption<SortDirection>>
                    aria-label="Sort direction"
                    className="sinks-sort-direction-select"
                    options={directionOptions}
                    value={directionOptions.find((x) => x.value === sort.direction)}
                    onChange={(x) => setSort({ ...sort, direction: x.value })}
                    isSearchable={false}
                    menuPortalTarget={null}
                    {...directionListEscape}
                />
            </div>
        </>
    );
}
