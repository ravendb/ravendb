import classNames from "classnames";
import React, { ChangeEvent, FocusEvent, KeyboardEvent, useRef, useState } from "react";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import { useScrollActiveIntoView } from "components/hooks/useScrollActiveIntoView";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import {
    DurationUnit,
    DurationUnitSymbols,
    formatDuration,
    getListedValues,
    getVisibleUnits,
    joinDuration,
    parseDuration,
    splitDuration,
} from "components/common/durationDropdownPickerUtils";
import useUniqueId from "components/hooks/useUniqueId";
import "./DurationDropdownPicker.scss";

const PAGE_JUMP = 5;

type DurationPlaceholder = Partial<Record<DurationUnit, string>>;

export interface DurationPickerProps {
    onChange: (totalSeconds: number) => void;
    totalSeconds: number;
    id?: string;
    showDays?: boolean;
    showSeconds?: boolean;
    disabled?: boolean;
    placeholder?: DurationPlaceholder;
    defaultTotalSeconds?: number;
    "aria-labelledby"?: string;
}

export const ZERO_DURATION_PLACEHOLDER: DurationPlaceholder = {
    days: "0",
    hours: "0",
    minutes: "0",
    seconds: "0",
};

export default function DurationDropdownPicker(props: DurationPickerProps) {
    const {
        totalSeconds,
        onChange,
        id,
        showDays,
        showSeconds,
        placeholder,
        defaultTotalSeconds,
        disabled,
        "aria-labelledby": ariaLabelledBy,
    } = props;

    const t = useStudioTranslation("durationPicker");
    const commonT = useStudioTranslation("common");
    const menuId = useUniqueId("duration-dropdown-picker-menu-");

    const units = getVisibleUnits(showDays, showSeconds);
    const symbols: DurationUnitSymbols = {
        days: t("daysSymbol"),
        hours: t("hoursSymbol"),
        minutes: t("minutesSymbol"),
        seconds: t("secondsSymbol"),
    };

    const [text, setText] = useState(() => formatDuration(totalSeconds, units, symbols));
    const [syncedTotalSeconds, setSyncedTotalSeconds] = useState(totalSeconds ?? null);
    const [lastReadableTotalSeconds, setLastReadableTotalSeconds] = useState(
        Number.isNaN(totalSeconds) ? null : (totalSeconds ?? null)
    );
    const [isOpen, setIsOpen] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);
    const menuRef = useRef<HTMLDivElement>(null);
    const shouldFocusListOnOpenRef = useRef(false);

    const consumerTotalSeconds = totalSeconds ?? null;
    if (!Object.is(consumerTotalSeconds, syncedTotalSeconds)) {
        setSyncedTotalSeconds(consumerTotalSeconds);
        if (!Number.isNaN(consumerTotalSeconds)) {
            setText(formatDuration(consumerTotalSeconds, units, symbols));
            setLastReadableTotalSeconds(consumerTotalSeconds);
        }
    }

    const commitTotalSeconds = (newTotalSeconds: number) => {
        setText(formatDuration(newTotalSeconds, units, symbols));
        setLastReadableTotalSeconds(newTotalSeconds);
        if (!Object.is(newTotalSeconds, syncedTotalSeconds)) {
            setSyncedTotalSeconds(newTotalSeconds);
            onChange(newTotalSeconds);
        }
    };

    const readText = (textToRead: string) => parseDuration(textToRead, units, symbols);

    const commitText = () => {
        const parsedTotalSeconds = readText(text);
        commitTotalSeconds(Number.isNaN(parsedTotalSeconds) ? lastReadableTotalSeconds : parsedTotalSeconds);
    };

    const close = () => {
        commitText();
        setIsOpen(false);
    };

    const hasDraft = lastReadableTotalSeconds != null;
    const draftParts = splitDuration(lastReadableTotalSeconds ?? 0, units);

    const handleTextChange = (newText: string) => {
        setText(newText);
        const parsedTotalSeconds = readText(newText);
        if (Number.isNaN(parsedTotalSeconds)) {
            return;
        }

        setLastReadableTotalSeconds(parsedTotalSeconds);
        if (!Object.is(parsedTotalSeconds, syncedTotalSeconds)) {
            setSyncedTotalSeconds(parsedTotalSeconds);
            onChange(parsedTotalSeconds);
        }
    };

    const pickValue = (unit: DurationUnit, value: number) => {
        commitTotalSeconds(joinDuration({ ...draftParts, [unit]: value }));
    };

    const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
        if (e.key === "Enter") {
            e.preventDefault();
            close();
        } else if (e.key === "Escape" && isOpen) {
            e.stopPropagation();
            close();
        } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
            e.preventDefault();
            if (isOpen) {
                focusColumn(getOptionColumns()[0]);
            } else {
                shouldFocusListOnOpenRef.current = true;
                setIsOpen(true);
            }
        }
    };

    const attachMenu = (menu: HTMLDivElement) => {
        menuRef.current = menu;
        if (menu && shouldFocusListOnOpenRef.current) {
            shouldFocusListOnOpenRef.current = false;
            focusColumn(getOptionColumns()[0]);
        }
    };

    const getOptionColumns = (): HTMLButtonElement[][] =>
        Array.from(menuRef.current?.querySelectorAll(".duration-dropdown-picker__list") ?? [], (list) =>
            Array.from(list.querySelectorAll("button"))
        );

    const focusColumn = (column: HTMLButtonElement[]) => {
        (column.find((option) => option.getAttribute("aria-pressed") === "true") ?? column[0])?.focus();
    };

    const handleMenuKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
        const columns = getOptionColumns();
        const columnIndex = columns.findIndex((column) => column.includes(e.target as HTMLButtonElement));
        if (columnIndex === -1) {
            return;
        }

        const column = columns[columnIndex];
        const optionIndex = column.indexOf(e.target as HTMLButtonElement);

        switch (e.key) {
            case "ArrowUp":
                column[Math.max(0, optionIndex - 1)].focus();
                break;
            case "ArrowDown":
                column[Math.min(column.length - 1, optionIndex + 1)].focus();
                break;
            case "PageUp":
                column[Math.max(0, optionIndex - PAGE_JUMP)].focus();
                break;
            case "PageDown":
                column[Math.min(column.length - 1, optionIndex + PAGE_JUMP)].focus();
                break;
            case "Home":
                column[0].focus();
                break;
            case "End":
                column.at(-1).focus();
                break;
            case "ArrowLeft":
                focusColumn(columns[columnIndex - 1] ?? column);
                break;
            case "ArrowRight":
                focusColumn(columns[columnIndex + 1] ?? column);
                break;
            case "Escape":
                e.stopPropagation();
                inputRef.current?.focus();
                close();
                break;
            default:
                return;
        }

        e.preventDefault();
    };

    const handleBlur = (e: FocusEvent<HTMLDivElement>) => {
        if (!containerRef.current?.contains(e.relatedTarget as Node)) {
            close();
        }
    };

    const getPlaceholder = (): string => {
        if (defaultTotalSeconds != null) {
            return commonT("defaultPlaceholder", { value: formatDuration(defaultTotalSeconds, units, symbols) });
        }
        if (placeholder) {
            return units.map((unit) => `${placeholder[unit] ?? ""}${symbols[unit]}`).join(" ");
        }
        return t("formatExample");
    };

    return (
        <div ref={containerRef} className="duration-dropdown-picker flex-grow-1" onBlur={handleBlur}>
            <Icon icon="clock" margin="m-0" className="duration-dropdown-picker__icon" />
            <Form.Control
                ref={inputRef}
                id={id}
                value={text}
                placeholder={getPlaceholder()}
                disabled={disabled}
                aria-labelledby={ariaLabelledBy}
                aria-expanded={isOpen}
                aria-haspopup="dialog"
                aria-controls={isOpen ? menuId : undefined}
                autoComplete="off"
                onChange={(e: ChangeEvent<HTMLInputElement>) => handleTextChange(e.currentTarget.value)}
                onFocus={() => setIsOpen(true)}
                onClick={() => setIsOpen(true)}
                onKeyDown={handleKeyDown}
            />
            {isOpen && !disabled && (
                <div
                    ref={attachMenu}
                    id={menuId}
                    className="duration-dropdown-picker__menu"
                    role="dialog"
                    aria-label={t("pickerLabel")}
                    onMouseDown={(e) => e.preventDefault()}
                    onKeyDown={handleMenuKeyDown}
                >
                    <div className="duration-dropdown-picker__columns">
                        {units.map((unit) => (
                            <DurationColumn
                                key={unit}
                                label={t(unit)}
                                values={getListedValues(unit, units, draftParts[unit])}
                                currentValue={hasDraft ? draftParts[unit] : null}
                                onPick={(value) => pickValue(unit, value)}
                            />
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}

interface DurationColumnProps {
    label: string;
    values: number[];
    currentValue: number;
    onPick: (value: number) => void;
}

function DurationColumn({ label, values, currentValue, onPick }: DurationColumnProps) {
    const { listRef, activeRef } = useScrollActiveIntoView<HTMLUListElement, HTMLButtonElement>(true);

    return (
        <div className="duration-dropdown-picker__column">
            <div className="duration-dropdown-picker__column-label">{label}</div>
            <ul className="duration-dropdown-picker__list" ref={listRef} aria-label={label} tabIndex={-1}>
                {values.map((value) => (
                    <li key={value}>
                        <button
                            type="button"
                            tabIndex={-1}
                            ref={value === currentValue ? activeRef : undefined}
                            className={classNames("duration-dropdown-picker__option", {
                                "duration-dropdown-picker__option--active": value === currentValue,
                            })}
                            aria-pressed={value === currentValue}
                            onClick={() => onPick(value)}
                        >
                            {value}
                        </button>
                    </li>
                ))}
            </ul>
        </div>
    );
}
