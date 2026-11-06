import "./FilterMenu.scss";
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
import IconName from "typings/server/icons";

export interface FilterOption<T> {
    value: T;
    label: string;
}

interface FilterCategoryBase {
    key: string;
    label: string;
    icon: IconName;
}

export interface TextFilterCategory extends FilterCategoryBase {
    type: "text";
    value: string;
    placeholder?: string;
    onChange(value: string): void;
}

export interface CheckboxListFilterCategory<T extends string = string> extends FilterCategoryBase {
    type: "checkboxList";
    options: FilterOption<T>[];
    selectedValues: T[];
    onChange(values: T[]): void;
}

export interface SingleChoiceFilterCategory<T = unknown> extends FilterCategoryBase {
    type: "singleChoice";
    options: FilterOption<T>[];
    selectedValue: T | null;
    onChange(value: T | null): void;
}

export type FilterCategory = TextFilterCategory | CheckboxListFilterCategory | SingleChoiceFilterCategory;

export function isFilterCategoryActive(category: FilterCategory): boolean {
    switch (category.type) {
        case "text":
            return category.value.trim() !== "";
        case "checkboxList":
            return category.selectedValues.length > 0;
        case "singleChoice":
            return category.selectedValue != null;
    }
}

function getFilterValueLabel(category: FilterCategory): string {
    switch (category.type) {
        case "text":
            return category.value;
        case "checkboxList":
            return category.options
                .filter((x) => category.selectedValues.includes(x.value))
                .map((x) => x.label)
                .join(", ");
        case "singleChoice":
            return category.options.find((x) => x.value === category.selectedValue)?.label;
    }
}

function clearFilterCategory(category: FilterCategory) {
    switch (category.type) {
        case "text":
            return category.onChange("");
        case "checkboxList":
            return category.onChange([]);
        case "singleChoice":
            return category.onChange(null);
    }
}

const filterMenuPopperConfig = { modifiers: [{ name: "offset", options: { offset: [0, 7] } }] };

interface FilterMenuProps {
    categories: FilterCategory[];
    title: string;
}

export function FilterMenu({ categories, title }: FilterMenuProps) {
    const [isOpen, setIsOpen] = useState(false);
    const [openCategoryKey, setOpenCategoryKey] = useState<string>(null);
    const categoryItems = useRef(new Map<string, HTMLElement>());
    const categoryKeyToRefocus = useRef<string>(null);

    useEffect(() => {
        if (openCategoryKey == null && categoryKeyToRefocus.current) {
            categoryItems.current.get(categoryKeyToRefocus.current)?.focus();
            categoryKeyToRefocus.current = null;
        }
    }, [openCategoryKey]);

    const goBackToCategories = () => {
        categoryKeyToRefocus.current = openCategoryKey;
        setOpenCategoryKey(null);
    };

    const toggleMenu = (nextIsOpen: boolean) => {
        setIsOpen(nextIsOpen);
        if (!nextIsOpen) {
            setOpenCategoryKey(null);
        }
    };

    const openCategory = categories.find((x) => x.key === openCategoryKey);
    const activeCategories = categories.filter(isFilterCategoryActive);
    const activeFiltersCount = activeCategories.length;

    return (
        <Dropdown as={ButtonGroup} show={isOpen} onToggle={toggleMenu} autoClose="outside" align="end">
            <Dropdown.Toggle
                as={CustomDropdownToggle}
                isCaretHidden
                title={title}
                aria-label={activeFiltersCount > 0 ? `${title} (${activeFiltersCount} active)` : title}
                className={classNames({ active: activeFiltersCount > 0 })}
            >
                <Icon icon="filter" margin="m-0" />
                {activeFiltersCount > 0 && (
                    <Badge pill bg="primary" className="filter-menu-count ms-1">
                        {activeFiltersCount}
                    </Badge>
                )}
            </Dropdown.Toggle>
            <Dropdown.Menu className="filter-menu-dropdown" popperConfig={filterMenuPopperConfig}>
                {openCategory ? (
                    <FilterCategoryMenu
                        category={openCategory}
                        isShown
                        onBack={goBackToCategories}
                        closeMenu={() => toggleMenu(false)}
                    />
                ) : (
                    <>
                        <MenuHeader
                            title="Filters"
                            actionLabel="Clear"
                            isActionVisible={activeFiltersCount > 0}
                            onAction={() => activeCategories.forEach(clearFilterCategory)}
                        />
                        {categories.map((category) => (
                            <Dropdown.Item
                                key={category.key}
                                ref={(element: HTMLButtonElement) => {
                                    categoryItems.current.set(category.key, element);
                                }}
                                as="button"
                                className="filter-menu-category-item"
                                onClick={() => setOpenCategoryKey(category.key)}
                            >
                                <Icon icon={category.icon} />
                                <span className="filter-menu-category-label">{category.label}</span>
                                {activeCategories.includes(category) && (
                                    <span className="filter-menu-active-dot">
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

interface FilterChipsProps {
    categories: FilterCategory[];
    className?: string;
    children?: ReactNode;
}

export function FilterChips({ categories, className, children }: FilterChipsProps) {
    const [openChipKey, setOpenChipKey] = useState<string>(null);

    const visibleCategories = categories.filter(
        (category) => category.key === openChipKey || isFilterCategoryActive(category)
    );

    if (visibleCategories.length === 0) {
        return null;
    }

    return (
        <div className={classNames("filter-chips", className)}>
            {visibleCategories.map((category) => (
                <FilterChip
                    key={category.key}
                    category={category}
                    isOpen={category.key === openChipKey}
                    setIsOpen={(isOpen) => setOpenChipKey(isOpen ? category.key : null)}
                />
            ))}
            {children}
        </div>
    );
}

interface FilterChipProps {
    category: FilterCategory;
    isOpen: boolean;
    setIsOpen: (isOpen: boolean) => void;
}

function FilterChip({ category, isOpen, setIsOpen }: FilterChipProps) {
    const clear = () => {
        setIsOpen(false);
        clearFilterCategory(category);
    };

    return (
        <Dropdown show={isOpen} onToggle={setIsOpen} autoClose="outside" className="filter-chip-dropdown">
            <div className="filter-chip">
                <Dropdown.Toggle
                    as="button"
                    type="button"
                    className="filter-chip-toggle"
                    title={`Change "${category.label}" filter`}
                >
                    <Icon icon={category.icon} />
                    <span className="filter-chip-label">{category.label}:</span>
                    <span className="filter-chip-value">{getFilterValueLabel(category)}</span>
                </Dropdown.Toggle>
                <button
                    type="button"
                    className="filter-chip-clear"
                    title={`Clear "${category.label}" filter`}
                    onClick={clear}
                >
                    <Icon icon="close" margin="m-0" />
                </button>
            </div>
            <Dropdown.Menu className="filter-menu-dropdown" popperConfig={filterMenuPopperConfig}>
                <FilterCategoryMenu category={category} isShown={isOpen} closeMenu={() => setIsOpen(false)} />
            </Dropdown.Menu>
        </Dropdown>
    );
}

interface FilterCategoryMenuProps {
    category: FilterCategory;
    isShown: boolean;
    closeMenu: () => void;
    onBack?: () => void;
}

function FilterCategoryMenu({ category, isShown, closeMenu, onBack }: FilterCategoryMenuProps) {
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
            <div className="filter-menu-header justify-content-start">
                {onBack && (
                    <Button variant="link" className="p-0 me-1" onClick={onBack} title="Back to filters">
                        <Icon icon="arrow-thin-left" margin="m-0" />
                    </Button>
                )}
                <span>{category.label}</span>
            </div>
            <div ref={optionsRef} role="group" aria-label={category.label} onKeyDown={moveBetweenCheckboxes}>
                <FilterCategoryControl category={category} closeMenu={closeMenu} />
            </div>
        </>
    );
}

function FilterCategoryControl({ category, closeMenu }: { category: FilterCategory; closeMenu: () => void }) {
    switch (category.type) {
        case "text":
            return <TextFilter category={category} onEnter={closeMenu} />;
        case "checkboxList":
            return <CheckboxListFilter category={category} onEnter={closeMenu} />;
        case "singleChoice":
            return <SingleChoiceFilter category={category} onEnter={closeMenu} />;
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

function TextFilter({ category, onEnter }: { category: TextFilterCategory; onEnter: () => void }) {
    return (
        <div className="filter-menu-field">
            <Form.Control
                type="text"
                placeholder={category.placeholder}
                value={category.value}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => category.onChange(e.target.value)}
                onKeyDown={handleEnterKey(onEnter)}
            />
        </div>
    );
}

function CheckboxListFilter({ category, onEnter }: { category: CheckboxListFilterCategory; onEnter: () => void }) {
    const { options, selectedValues, onChange } = category;

    const toggleValue = (value: string) => {
        onChange(
            selectedValues.includes(value) ? selectedValues.filter((x) => x !== value) : [...selectedValues, value]
        );
    };

    return (
        <div>
            {options.map((option) => (
                <div key={option.value} className="filter-menu-checkbox">
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

function SingleChoiceFilter({ category, onEnter }: { category: SingleChoiceFilterCategory; onEnter: () => void }) {
    const { options, selectedValue, onChange } = category;
    const pickAndClose = (value: unknown) => {
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
                        className="filter-menu-option"
                        aria-pressed={isSelected}
                        onClick={() => onChange(option.value)}
                        onKeyDown={handleEnterKey(() => pickAndClose(option.value))}
                    >
                        {option.label}
                        {isSelected && <Icon icon="check" margin="ms-auto" />}
                    </Dropdown.Item>
                );
            })}
        </>
    );
}

interface MenuHeaderProps {
    title: string;
    actionLabel: string;
    isActionVisible: boolean;
    onAction: () => void;
}

export function MenuHeader({ title, actionLabel, isActionVisible, onAction }: MenuHeaderProps) {
    return (
        <div className="filter-menu-header">
            <span>{title}</span>
            <Button
                variant="link"
                size="xs"
                className={classNames("p-0", { invisible: !isActionVisible })}
                onClick={onAction}
                disabled={!isActionVisible}
            >
                <Icon icon="reset" />
                {actionLabel}
            </Button>
        </div>
    );
}
