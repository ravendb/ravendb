import React, { ChangeEvent, useRef } from "react";
import moment from "moment";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useClickOutside } from "components/hooks/useClickOutside";
import { ColumnPickerColumn, ColumnPickerMenu } from "components/common/ColumnPicker";
import useBoolean from "components/hooks/useBoolean";
import { FilterTimezone } from "./timeSeriesRangeUtils";

export const TIME_FORMAT = "HH:mm:ss.SSS";
export const TIME_PARSE_FORMATS = ["HH:mm:ss.SSS", "HH:mm:ss", "HH:mm"];

interface TimePickerProps {
    value: string;
    invalid: boolean;
    timezone: FilterTimezone;
    onChange: (time: string) => void;
}

interface TimeParts {
    h: number;
    m: number;
    s: number;
    ms: number;
}

const HOUR_VALUES = Array.from({ length: 24 }, (_, i) => i);
const MINUTE_VALUES = Array.from({ length: 60 }, (_, i) => i);
const SECOND_VALUES = Array.from({ length: 60 }, (_, i) => i);

function pad2(n: number): string {
    return String(n).padStart(2, "0");
}

function parseTimeParts(text: string): TimeParts | null {
    const trimmed = text?.trim();
    if (!trimmed) {
        return null;
    }
    const parsed = moment(trimmed, TIME_PARSE_FORMATS, true);
    if (!parsed.isValid()) {
        return null;
    }
    return { h: parsed.hour(), m: parsed.minute(), s: parsed.second(), ms: parsed.millisecond() };
}

function formatTimeParts(parts: TimeParts): string {
    return `${pad2(parts.h)}:${pad2(parts.m)}:${pad2(parts.s)}.${String(parts.ms).padStart(3, "0")}`;
}

// Replaces a single component while preserving the millisecond part the columns don't expose,
// so clicking a column never discards typed ms.
function withTimePart(text: string, unit: keyof TimeParts, value: number): string {
    const parts = parseTimeParts(text) ?? { h: 0, m: 0, s: 0, ms: 0 };
    parts[unit] = value;
    return formatTimeParts(parts);
}

interface TimeColumnDef {
    label: string;
    unit: keyof TimeParts;
    values: number[];
    current: number | null;
}

// Free-form text input (the source of truth, so exact HH:mm:ss.SSS can always be typed) plus a
// click-to-pick Hour/Minute/Second dropdown. 24-hour, no native <select> (see HeaderSelect).
export default function TimePicker({ value, invalid, timezone, onChange }: TimePickerProps) {
    const { value: open, setTrue: openMenu, setFalse: closeMenu } = useBoolean(false);
    const ref = useRef<HTMLDivElement>(null);

    useClickOutside(ref, open, closeMenu);

    const parts = parseTimeParts(value);
    const columns: TimeColumnDef[] = [
        { label: "Hour", unit: "h", values: HOUR_VALUES, current: parts?.h ?? null },
        { label: "Min", unit: "m", values: MINUTE_VALUES, current: parts?.m ?? null },
        { label: "Sec", unit: "s", values: SECOND_VALUES, current: parts?.s ?? null },
    ];

    const nowText = () => (timezone === "utc" ? moment.utc() : moment()).format(TIME_FORMAT);

    return (
        <div className="ts-range-picker__input-icon ts-time-picker" ref={ref}>
            <Icon icon="clock" margin="m-0" className="ts-range-picker__input-icon-glyph" />
            <Form.Control
                value={value}
                isInvalid={invalid}
                placeholder={TIME_FORMAT}
                onChange={(e: ChangeEvent<HTMLInputElement>) => onChange(e.target.value)}
                onFocus={openMenu}
                onBlur={() => {
                    // Normalise a partial-but-valid entry to the full format so buildInstant's
                    // stricter parse never rejects text this field just accepted.
                    const normalized = parseTimeParts(value);
                    if (normalized) {
                        onChange(formatTimeParts(normalized));
                    }
                }}
            />
            {open && (
                <ColumnPickerMenu
                    className="ts-time-picker__menu"
                    footer={
                        <div className="ts-time-picker__footer">
                            <button
                                type="button"
                                className="ts-time-picker__action"
                                onClick={() => onChange(nowText())}
                            >
                                Now
                            </button>
                            <Button variant="primary" size="sm" onClick={closeMenu}>
                                Apply
                            </Button>
                        </div>
                    }
                >
                    {columns.map((col) => (
                        <ColumnPickerColumn
                            key={col.unit}
                            label={col.label}
                            values={col.values}
                            currentValue={col.current}
                            formatValue={pad2}
                            isLabelVisible={false}
                            onPick={(v) => onChange(withTimePart(value, col.unit, v))}
                        />
                    ))}
                </ColumnPickerMenu>
            )}
        </div>
    );
}
