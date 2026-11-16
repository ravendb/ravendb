import React, { ChangeEvent, FocusEvent, KeyboardEvent, useRef, useState } from "react";
import Form from "react-bootstrap/Form";
import { ColumnPickerColumn, ColumnPickerMenu, focusFirstColumn } from "components/common/ColumnPicker";
import { Icon } from "components/common/Icon";
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
} from "components/common/durationPickerUtils";
import useUniqueId from "components/hooks/useUniqueId";
import "./DurationPicker.scss";

type DurationPlaceholder = Partial<Record<DurationUnit, string>>;

export interface DurationPickerProps {
    onChange: (totalSeconds: number) => void;
    totalSeconds: number;
    id?: string;
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

export default function DurationPicker(props: DurationPickerProps) {
    const {
        totalSeconds,
        onChange,
        id,
        showSeconds,
        placeholder,
        defaultTotalSeconds,
        disabled,
        "aria-labelledby": ariaLabelledBy,
    } = props;

    const t = useStudioTranslation("durationPicker");
    const commonT = useStudioTranslation("common");
    const menuId = useUniqueId("duration-picker-menu-");

    const units = getVisibleUnits(showSeconds);
    const symbols: DurationUnitSymbols = {
        days: t("daysSymbol"),
        hours: t("hoursSymbol"),
        minutes: t("minutesSymbol"),
        seconds: t("secondsSymbol"),
    };

    const [text, setText] = useState(() => formatDuration(totalSeconds, units, symbols));
    const [syncedTotalSeconds, setSyncedTotalSeconds] = useState(totalSeconds ?? null);
    const [isOpen, setIsOpen] = useState(false);
    const containerRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);
    const shouldFocusListOnOpenRef = useRef(false);
    const menuRef = useRef<HTMLDivElement>(null);

    const consumerTotalSeconds = totalSeconds ?? null;
    if (consumerTotalSeconds !== syncedTotalSeconds) {
        setSyncedTotalSeconds(consumerTotalSeconds);
        setText(formatDuration(consumerTotalSeconds, units, symbols));
    }

    const commitTotalSeconds = (newTotalSeconds: number) => {
        setText(formatDuration(newTotalSeconds, units, symbols));
        if (newTotalSeconds !== syncedTotalSeconds) {
            setSyncedTotalSeconds(newTotalSeconds);
            onChange(newTotalSeconds);
        }
    };

    const readText = (textToRead: string) => parseDuration(textToRead, units, symbols);

    const commitText = () => {
        const parsedTotalSeconds = readText(text);
        commitTotalSeconds(Number.isNaN(parsedTotalSeconds) ? syncedTotalSeconds : parsedTotalSeconds);
    };

    const close = () => {
        commitText();
        setIsOpen(false);
    };

    const hasValue = syncedTotalSeconds != null;
    const valueParts = splitDuration(syncedTotalSeconds ?? 0, units);

    const handleTextChange = (newText: string) => {
        setText(newText);
        const parsedTotalSeconds = readText(newText);
        if (Number.isNaN(parsedTotalSeconds) || parsedTotalSeconds === syncedTotalSeconds) {
            return;
        }

        setSyncedTotalSeconds(parsedTotalSeconds);
        onChange(parsedTotalSeconds);
    };

    const pickValue = (unit: DurationUnit, value: number) => {
        commitTotalSeconds(joinDuration({ ...valueParts, [unit]: value }));
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
                focusFirstColumn(menuRef.current);
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
            focusFirstColumn(menu);
        }
    };

    const handleMenuKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
        if (e.key === "Escape") {
            e.preventDefault();
            e.stopPropagation();
            inputRef.current?.focus();
            close();
        }
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
        <div ref={containerRef} className="duration-picker flex-grow-1" onBlur={handleBlur}>
            <Icon icon="clock" margin="m-0" className="duration-picker__icon" />
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
                <ColumnPickerMenu
                    ref={attachMenu}
                    id={menuId}
                    role="dialog"
                    aria-label={t("pickerLabel")}
                    onMouseDown={(e) => e.preventDefault()}
                    onKeyDown={handleMenuKeyDown}
                >
                    {units.map((unit) => (
                        <ColumnPickerColumn
                            key={unit}
                            label={t(unit)}
                            values={getListedValues(unit, valueParts[unit])}
                            currentValue={hasValue ? valueParts[unit] : null}
                            onPick={(value) => pickValue(unit, value)}
                            optionTabIndex={-1}
                        />
                    ))}
                </ColumnPickerMenu>
            )}
        </div>
    );
}
