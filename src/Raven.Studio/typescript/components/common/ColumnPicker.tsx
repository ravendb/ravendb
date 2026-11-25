import classNames from "classnames";
import React, { HTMLAttributes, KeyboardEvent, ReactNode, Ref } from "react";
import { useScrollActiveIntoView } from "components/hooks/useScrollActiveIntoView";
import "./ColumnPicker.scss";

const PAGE_JUMP = 5;

interface ColumnPickerMenuProps extends HTMLAttributes<HTMLDivElement> {
    ref?: Ref<HTMLDivElement>;
    footer?: ReactNode;
}

export function ColumnPickerMenu({ children, footer, className, onKeyDown, ...rest }: ColumnPickerMenuProps) {
    const handleKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
        if (moveFocusWithinColumns(e)) {
            e.preventDefault();
            return;
        }
        onKeyDown?.(e);
    };

    return (
        <div className={classNames("column-picker", className)} onKeyDown={handleKeyDown} {...rest}>
            <div className="column-picker__columns">{children}</div>
            {footer}
        </div>
    );
}

interface ColumnPickerColumnProps {
    label: string;
    values: number[];
    currentValue: number;
    onPick: (value: number) => void;
    formatValue?: (value: number) => string;
    isLabelVisible?: boolean;
    optionTabIndex?: number;
}

export function ColumnPickerColumn({
    label,
    values,
    currentValue,
    onPick,
    formatValue = String,
    isLabelVisible = true,
    optionTabIndex,
}: ColumnPickerColumnProps) {
    const { listRef, activeRef } = useScrollActiveIntoView<HTMLUListElement, HTMLButtonElement>(true);

    return (
        <div className="column-picker__column">
            {isLabelVisible && <div className="column-picker__column-label">{label}</div>}
            <ul className="column-picker__list" ref={listRef} aria-label={label} tabIndex={-1}>
                {values.map((value) => (
                    <li key={value}>
                        <button
                            type="button"
                            tabIndex={optionTabIndex}
                            ref={value === currentValue ? activeRef : undefined}
                            className={classNames("column-picker__option", {
                                "column-picker__option--active": value === currentValue,
                            })}
                            aria-pressed={value === currentValue}
                            onClick={() => onPick(value)}
                        >
                            {formatValue(value)}
                        </button>
                    </li>
                ))}
            </ul>
        </div>
    );
}

export function focusFirstColumn(menu: HTMLElement) {
    focusColumn(getOptionColumns(menu)[0]);
}

function getOptionColumns(menu: HTMLElement): HTMLButtonElement[][] {
    return Array.from(menu?.querySelectorAll(".column-picker__list") ?? [], (list) =>
        Array.from(list.querySelectorAll("button"))
    );
}

function focusColumn(column: HTMLButtonElement[]) {
    (column?.find((option) => option.getAttribute("aria-pressed") === "true") ?? column?.[0])?.focus();
}

function moveFocusWithinColumns(e: KeyboardEvent<HTMLDivElement>): boolean {
    const columns = getOptionColumns(e.currentTarget);
    const columnIndex = columns.findIndex((column) => column.includes(e.target as HTMLButtonElement));
    if (columnIndex === -1) {
        return false;
    }

    const column = columns[columnIndex];
    const optionIndex = column.indexOf(e.target as HTMLButtonElement);

    switch (e.key) {
        case "ArrowUp":
            column[Math.max(0, optionIndex - 1)].focus();
            return true;
        case "ArrowDown":
            column[Math.min(column.length - 1, optionIndex + 1)].focus();
            return true;
        case "PageUp":
            column[Math.max(0, optionIndex - PAGE_JUMP)].focus();
            return true;
        case "PageDown":
            column[Math.min(column.length - 1, optionIndex + PAGE_JUMP)].focus();
            return true;
        case "Home":
            column[0].focus();
            return true;
        case "End":
            column.at(-1).focus();
            return true;
        case "ArrowLeft":
            focusColumn(columns[columnIndex - 1] ?? column);
            return true;
        case "ArrowRight":
            focusColumn(columns[columnIndex + 1] ?? column);
            return true;
        default:
            return false;
    }
}
