import React, { ReactNode, useEffect, useRef, useState } from "react";
import classNames from "classnames";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { CustomDropdownToggle } from "components/common/Dropdown";
import Select, {
    OptionWithIcon,
    SelectOption,
    SelectOptionWithIcon,
    SingleValueWithIcon,
} from "components/common/select/Select";
import { SortDirection } from "components/models/common";
import {
    getSinkFilterValueLabel,
    ReplicationHubSinksState,
    SinkFilterCategory,
    SinkFilterCategoryInfo,
    sinkConnectionStatusLabels,
    sinkFilterCategories,
    sinkReplicationStateLabels,
    SinkSortField,
    sinkSortFields,
} from "./useReplicationHubSinks";
import { ReplicationProgressState } from "./ExternalReplicationTaskDistribution";
import "./ReplicationHubSinksHeader.scss";
import OngoingTaskConnectionStatus = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus;

interface ReplicationHubSinksHeaderProps {
    sinksState: ReplicationHubSinksState;
}

function formatSinksCount(count: number): string {
    return `${count} ${count === 1 ? "sink" : "sinks"}`;
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
        activeFilterCategories,
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
                    <FiltersDropdown sinksState={sinksState} />
                    <Button
                        variant="secondary"
                        onClick={() => keepTopVisibleSinkInPlace(headerRef.current, toggleAllExpanded)}
                        title={areAllVisibleSinksExpanded ? "Collapse all sinks" : "Expand all sinks"}
                    >
                        <Icon icon={areAllVisibleSinksExpanded ? "fold" : "unfold"} margin="m-0" />
                    </Button>
                </ButtonGroup>
            </div>
            {hasActiveFilters && (
                <div className="replication-hub-sinks-chips">
                    {activeFilterCategories.map((categoryInfo) => (
                        <FilterChip key={categoryInfo.category} categoryInfo={categoryInfo} sinksState={sinksState} />
                    ))}
                    <span className="sinks-results-count">
                        <strong>{visibleSinks.length}</strong> of {formatSinksCount(totalSinksCount)}
                    </span>
                </div>
            )}
            <div className="visually-hidden" role="status">
                {hasActiveFilters ? `${visibleSinks.length} of ${formatSinksCount(totalSinksCount)} shown` : ""}
            </div>
        </>
    );
}

function getSortFieldInfo(field: SinkSortField) {
    return sinkSortFields.find((x) => x.field === field);
}

const sinksMenuPopperConfig = { modifiers: [{ name: "offset", options: { offset: [0, 7] } }] };

const sortFieldOptions: SelectOptionWithIcon<SinkSortField>[] = sinkSortFields.map(({ field, label, icon }) => ({
    value: field,
    label,
    icon,
}));

const sortDirectionOptions: SelectOption<SortDirection>[] = [
    { value: "asc", label: "Ascending" },
    { value: "desc", label: "Descending" },
];

function SortDropdown({ sinksState }: { sinksState: ReplicationHubSinksState }) {
    const { sort } = sinksState;
    const fieldLabel = getSortFieldInfo(sort.field).label;
    const directionLabel = sortDirectionOptions.find((x) => x.value === sort.direction).label;

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
            <Dropdown.Menu className="sinks-dropdown-menu" popperConfig={sinksMenuPopperConfig}>
                <SortMenu sinksState={sinksState} />
            </Dropdown.Menu>
        </Dropdown>
    );
}

function SortMenu({ sinksState }: { sinksState: ReplicationHubSinksState }) {
    const { sort, setSort } = sinksState;

    return (
        <div className="sinks-sort-selects">
            <Select<SelectOptionWithIcon<SinkSortField>>
                aria-label="Sort by"
                className="sinks-sort-field-select"
                options={sortFieldOptions}
                value={sortFieldOptions.find((x) => x.value === sort.field)}
                onChange={(x) => setSort({ ...sort, field: x.value })}
                components={{ Option: OptionWithIcon, SingleValue: SingleValueWithIcon }}
                isSearchable={false}
            />
            <Select<SelectOption<SortDirection>>
                aria-label="Sort direction"
                className="sinks-sort-direction-select"
                options={sortDirectionOptions}
                value={sortDirectionOptions.find((x) => x.value === sort.direction)}
                onChange={(x) => setSort({ ...sort, direction: x.value })}
                isSearchable={false}
            />
        </div>
    );
}

interface ChipProps {
    label: ReactNode;
    toggleTitle: string;
    clearTitle: string;
    onClear: () => void;
    isOpen: boolean;
    onToggle: (isOpen: boolean) => void;
    children: ReactNode;
}

function Chip({ label, toggleTitle, clearTitle, onClear, isOpen, onToggle, children }: ChipProps) {
    return (
        <Dropdown show={isOpen} onToggle={onToggle} autoClose="outside" className="sinks-chip-dropdown">
            <div className="sinks-chip">
                <Dropdown.Toggle as="button" type="button" className="sinks-chip-toggle" title={toggleTitle}>
                    {label}
                </Dropdown.Toggle>
                <button type="button" className="sinks-chip-clear" title={clearTitle} onClick={onClear}>
                    <Icon icon="close" margin="m-0" />
                </button>
            </div>
            <Dropdown.Menu className="sinks-dropdown-menu" popperConfig={sinksMenuPopperConfig}>
                {children}
            </Dropdown.Menu>
        </Dropdown>
    );
}

function FiltersDropdown({ sinksState }: { sinksState: ReplicationHubSinksState }) {
    const { activeFilterCategories, clearFilters } = sinksState;
    const [isOpen, setIsOpen] = useState(false);
    const [openCategory, setOpenCategory] = useState<SinkFilterCategory>(null);
    const categoryItems = useRef(new Map<SinkFilterCategory, HTMLElement>());
    const categoryToRefocus = useRef<SinkFilterCategory>(null);

    useEffect(() => {
        if (openCategory == null && categoryToRefocus.current) {
            categoryItems.current.get(categoryToRefocus.current)?.focus();
            categoryToRefocus.current = null;
        }
    }, [openCategory]);

    const goBackToCategories = () => {
        categoryToRefocus.current = openCategory;
        setOpenCategory(null);
    };

    const toggleMenu = (nextIsOpen: boolean) => {
        setIsOpen(nextIsOpen);
        if (!nextIsOpen) {
            setOpenCategory(null);
        }
    };

    const openCategoryInfo = sinkFilterCategories.find((x) => x.category === openCategory);
    const activeFiltersCount = activeFilterCategories.length;

    return (
        <Dropdown as={ButtonGroup} show={isOpen} onToggle={toggleMenu} autoClose="outside" align="end">
            <Dropdown.Toggle
                as={CustomDropdownToggle}
                isCaretHidden
                title="Filter sinks"
                aria-label={activeFiltersCount > 0 ? `Filter sinks (${activeFiltersCount} active)` : "Filter sinks"}
                className={classNames({ active: activeFiltersCount > 0 })}
            >
                <Icon icon="filter" margin="m-0" />
                {activeFiltersCount > 0 && (
                    <Badge pill bg="primary" className="sinks-filter-count ms-1">
                        {activeFiltersCount}
                    </Badge>
                )}
            </Dropdown.Toggle>
            <Dropdown.Menu className="sinks-dropdown-menu" popperConfig={sinksMenuPopperConfig}>
                {openCategoryInfo ? (
                    <FilterCategoryMenu
                        categoryInfo={openCategoryInfo}
                        sinksState={sinksState}
                        isShown
                        onBack={goBackToCategories}
                        closeMenu={() => toggleMenu(false)}
                    />
                ) : (
                    <>
                        <div className="sinks-dropdown-header">
                            <span>Filters</span>
                            <ClearButton isVisible={activeFilterCategories.length > 0} onClick={clearFilters} />
                        </div>
                        {sinkFilterCategories.map(({ category, label, icon }) => (
                            <Dropdown.Item
                                key={category}
                                ref={(element: HTMLButtonElement) => {
                                    categoryItems.current.set(category, element);
                                }}
                                as="button"
                                className="sinks-filter-category-item"
                                onClick={() => setOpenCategory(category)}
                            >
                                <Icon icon={icon} />
                                <span className="sinks-filter-category-label">{label}</span>
                                {activeFilterCategories.some((x) => x.category === category) && (
                                    <span className="sinks-filter-active-dot">
                                        <span className="visually-hidden"> (active)</span>
                                    </span>
                                )}
                            </Dropdown.Item>
                        ))}
                    </>
                )}
            </Dropdown.Menu>
        </Dropdown>
    );
}

function FilterChip({
    categoryInfo,
    sinksState,
}: {
    categoryInfo: SinkFilterCategoryInfo;
    sinksState: ReplicationHubSinksState;
}) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <Chip
            isOpen={isOpen}
            onToggle={setIsOpen}
            label={
                <>
                    <Icon icon={categoryInfo.icon} />
                    <span className="sinks-chip-label">{categoryInfo.label}:</span>
                    <span className="sinks-chip-value">
                        {getSinkFilterValueLabel(sinksState.filters, categoryInfo.category)}
                    </span>
                </>
            }
            toggleTitle={`Change "${categoryInfo.label}" filter`}
            clearTitle={`Clear "${categoryInfo.label}" filter`}
            onClear={() => sinksState.clearFilter(categoryInfo.category)}
        >
            <FilterCategoryMenu
                categoryInfo={categoryInfo}
                sinksState={sinksState}
                isShown={isOpen}
                closeMenu={() => setIsOpen(false)}
            />
        </Chip>
    );
}

interface FilterCategoryMenuProps {
    categoryInfo: SinkFilterCategoryInfo;
    sinksState: ReplicationHubSinksState;
    isShown: boolean;
    closeMenu: () => void;
    onBack?: () => void;
}

function FilterCategoryMenu({ categoryInfo, sinksState, isShown, closeMenu, onBack }: FilterCategoryMenuProps) {
    const optionsRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (isShown) {
            optionsRef.current?.querySelector<HTMLElement>("input, button")?.focus({ preventScroll: true });
        }
    }, [isShown]);

    const moveBetweenCheckboxes = (e: React.KeyboardEvent) => {
        const target = e.target as HTMLElement;
        const isCheckbox = target instanceof HTMLInputElement && target.type === "checkbox";

        if ((e.key === "ArrowDown" || e.key === "ArrowUp") && isCheckbox) {
            e.preventDefault();
            const checkboxes = Array.from(
                optionsRef.current.querySelectorAll<HTMLInputElement>('input[type="checkbox"]')
            );
            const step = e.key === "ArrowDown" ? 1 : -1;
            checkboxes[checkboxes.indexOf(target) + step]?.focus();
        }
    };

    return (
        <>
            <div className="sinks-dropdown-header justify-content-start">
                {onBack && (
                    <Button variant="link" className="p-0 me-1" onClick={onBack} title="Back to filters">
                        <Icon icon="arrow-thin-left" margin="m-0" />
                    </Button>
                )}
                <span>{categoryInfo.label}</span>{" "}
            </div>
            <div ref={optionsRef} role="group" aria-label={categoryInfo.label} onKeyDown={moveBetweenCheckboxes}>
                <FilterCategoryMenuBody
                    category={categoryInfo.category}
                    sinksState={sinksState}
                    closeMenu={closeMenu}
                />
            </div>
        </>
    );
}

function FilterCategoryMenuBody({
    category,
    sinksState,
    closeMenu,
}: {
    category: SinkFilterCategory;
    sinksState: ReplicationHubSinksState;
    closeMenu: () => void;
}) {
    const { filters, updateFilters, nodeTags } = sinksState;

    switch (category) {
        case "databaseName":
            return (
                <TextFilter
                    value={filters.databaseName}
                    placeholder="e.g. name"
                    onChange={(databaseName) => updateFilters({ databaseName })}
                    onEnter={closeMenu}
                />
            );
        case "url":
            return (
                <TextFilter
                    value={filters.url}
                    placeholder="e.g. https://"
                    onChange={(url) => updateFilters({ url })}
                    onEnter={closeMenu}
                />
            );
        case "state":
            return (
                <CheckboxListFilter
                    options={Object.entries(sinkReplicationStateLabels).map(([value, label]) => ({
                        value: value as ReplicationProgressState,
                        label,
                    }))}
                    selectedValues={filters.states}
                    onChange={(states) => updateFilters({ states })}
                    onEnter={closeMenu}
                />
            );
        case "status":
            return (
                <CheckboxListFilter
                    options={Object.entries(sinkConnectionStatusLabels).map(([value, label]) => ({
                        value: value as OngoingTaskConnectionStatus,
                        label,
                    }))}
                    selectedValues={filters.statuses}
                    onChange={(statuses) => updateFilters({ statuses })}
                    onEnter={closeMenu}
                />
            );
        case "hasErrors":
            return (
                <SingleChoiceFilter
                    options={hasErrorsOptions}
                    selectedValue={filters.hasErrors}
                    onChange={(hasErrors) => updateFilters({ hasErrors })}
                    onEnter={closeMenu}
                />
            );
        case "node":
            return (
                <CheckboxListFilter
                    options={nodeTags.map((x) => ({ value: x, label: `Node ${x}` }))}
                    selectedValues={filters.nodeTags}
                    onChange={(selectedNodeTags) => updateFilters({ nodeTags: selectedNodeTags })}
                    onEnter={closeMenu}
                />
            );
    }
}

function handleEnterKey(action: () => void) {
    return (e: React.KeyboardEvent) => {
        if (e.key === "Enter") {
            e.preventDefault();
            action();
        }
    };
}

interface TextFilterProps {
    value: string;
    placeholder: string;
    onChange: (value: string) => void;
    onEnter: () => void;
}

function TextFilter({ value, placeholder, onChange, onEnter }: TextFilterProps) {
    return (
        <div className="px-3 py-1">
            <Form.Control
                type="text"
                placeholder={placeholder}
                value={value}
                onChange={(e) => onChange(e.target.value)}
                onKeyDown={handleEnterKey(onEnter)}
            />
        </div>
    );
}

interface CheckboxListFilterProps<T extends string> {
    options: { value: T; label: string }[];
    selectedValues: T[];
    onChange: (values: T[]) => void;
    onEnter: () => void;
}

function CheckboxListFilter<T extends string>({
    options,
    selectedValues,
    onChange,
    onEnter,
}: CheckboxListFilterProps<T>) {
    const toggleValue = (value: T) => {
        onChange(
            selectedValues.includes(value) ? selectedValues.filter((x) => x !== value) : [...selectedValues, value]
        );
    };

    return (
        <div>
            {options.map((option) => (
                <div key={option.value} className="px-3 py-1">
                    <Checkbox
                        selected={selectedValues.includes(option.value)}
                        toggleSelection={() => toggleValue(option.value)}
                        onKeyDown={handleEnterKey(() => {
                            toggleValue(option.value);
                            onEnter();
                        })}
                    >
                        {option.label}
                    </Checkbox>
                </div>
            ))}
        </div>
    );
}

const hasErrorsOptions: { value: boolean; label: string }[] = [
    { value: true, label: "Yes" },
    { value: false, label: "No" },
];

interface SingleChoiceFilterProps<T> {
    options: { value: T; label: string }[];
    selectedValue: T | null;
    onChange: (value: T | null) => void;
    onEnter: () => void;
}

function SingleChoiceFilter<T>({ options, selectedValue, onChange, onEnter }: SingleChoiceFilterProps<T>) {
    const hasSelection = selectedValue != null;

    const pickAndClose = (value: T | null) => {
        onChange(value);
        onEnter();
    };

    return (
        <>
            {options.map((option) => {
                const isSelected = option.value === selectedValue;
                return (
                    <Dropdown.Item
                        key={String(option.value)}
                        as="button"
                        type="button"
                        className="sinks-filter-option"
                        aria-pressed={isSelected}
                        onClick={() => onChange(option.value)}
                        onKeyDown={handleEnterKey(() => pickAndClose(option.value))}
                    >
                        {option.label}
                        {isSelected && <Icon icon="check" margin="ms-auto" />}
                    </Dropdown.Item>
                );
            })}
            <Dropdown.Divider />
            <Dropdown.Item
                as="button"
                type="button"
                disabled={!hasSelection}
                onClick={() => onChange(null)}
                onKeyDown={hasSelection ? handleEnterKey(() => pickAndClose(null)) : undefined}
            >
                <Icon icon="reset" />
                Clear
            </Dropdown.Item>
        </>
    );
}

function ClearButton({ isVisible, onClick }: { isVisible: boolean; onClick: () => void }) {
    return (
        <Button
            variant="link"
            size="xs"
            className={classNames("p-0", { invisible: !isVisible })}
            onClick={onClick}
            disabled={!isVisible}
        >
            <Icon icon="reset" />
            Clear
        </Button>
    );
}
