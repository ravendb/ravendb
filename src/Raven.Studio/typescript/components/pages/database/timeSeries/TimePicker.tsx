import React, { useEffect, useRef, useState } from "react";
import moment from "moment";
import classNames from "classnames";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";

export const TIME_FORMAT = "HH:mm:ss.SSS";
// Accepted when parsing typed time text - lenient about missing seconds/ms. Displayed/normalised
// text always uses the full TIME_FORMAT.
export const TIME_PARSE_FORMATS = ["HH:mm:ss.SSS", "HH:mm:ss", "HH:mm"];

interface TimePickerProps {
    value: string;
    invalid: boolean;
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

// Parse the typed time into its components. Lenient about missing seconds/ms so the column
// highlight tracks partially-typed values; returns null only when nothing usable is present.
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

// Replace a single component (hour/minute/second) while preserving the rest — crucially the
// millisecond part the columns don't expose, so clicking a column never discards typed ms.
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

// A single scrollable column of two-digit values (hours / minutes / seconds). Scrolls the active
// value into the middle when the menu opens so the current selection is visible without hunting.
function TimeColumn({ label, values, current, onPick }: TimeColumnDef & { onPick: (value: number) => void }) {
    const listRef = useRef<HTMLUListElement>(null);
    const activeRef = useRef<HTMLButtonElement>(null);

    useEffect(() => {
        const list = listRef.current;
        const active = activeRef.current;
        if (list && active) {
            list.scrollTop = active.offsetTop - list.clientHeight / 2 + active.clientHeight / 2;
        }
    }, []);

    return (
        <div className="ts-time-picker__col">
            <ul className="ts-time-picker__list" ref={listRef} aria-label={label}>
                {values.map((v) => (
                    <li key={v}>
                        <button
                            type="button"
                            ref={v === current ? activeRef : undefined}
                            className={classNames("ts-time-picker__option", {
                                "ts-time-picker__option--active": v === current,
                            })}
                            onClick={() => onPick(v)}
                        >
                            {pad2(v)}
                        </button>
                    </li>
                ))}
            </ul>
        </div>
    );
}

// Time entry that keeps a free-form text input (the source of truth, so exact HH:mm:ss.SSS —
// including milliseconds — can always be typed) and adds a click-to-pick dropdown of Hour/Minute/
// Second columns for quick coarse selection. 24-hour, no AM/PM. No native <select> for the same
// reason as HeaderSelect in RangeDatePickerHeader.
export default function TimePicker({ value, invalid, onChange }: TimePickerProps) {
    const [open, setOpen] = useState(false);
    const ref = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!open) {
            return;
        }
        const handleOutside = (e: MouseEvent) => {
            if (ref.current && !ref.current.contains(e.target as Node)) {
                setOpen(false);
            }
        };
        document.addEventListener("mousedown", handleOutside);
        return () => document.removeEventListener("mousedown", handleOutside);
    }, [open]);

    const parts = parseTimeParts(value);
    const columns: TimeColumnDef[] = [
        { label: "Hour", unit: "h", values: HOUR_VALUES, current: parts?.h ?? null },
        { label: "Min", unit: "m", values: MINUTE_VALUES, current: parts?.m ?? null },
        { label: "Sec", unit: "s", values: SECOND_VALUES, current: parts?.s ?? null },
    ];

    return (
        <div className="ts-range-picker__input-icon ts-time-picker" ref={ref}>
            <Icon icon="clock" margin="m-0" className="ts-range-picker__input-icon-glyph" />
            <Form.Control
                value={value}
                isInvalid={invalid}
                placeholder={TIME_FORMAT}
                onChange={(e) => onChange(e.target.value)}
                onFocus={() => setOpen(true)}
                onBlur={() => {
                    // Normalise a partial-but-valid entry (e.g. "14:30") to the full HH:mm:ss.SSS
                    // shown as the placeholder, so buildInstant's stricter parse never rejects
                    // text this same field just accepted.
                    const normalized = parseTimeParts(value);
                    if (normalized) {
                        onChange(formatTimeParts(normalized));
                    }
                }}
            />
            {open && (
                <div className="ts-time-picker__menu">
                    <div className="ts-time-picker__columns">
                        {columns.map((col) => (
                            <TimeColumn
                                key={col.unit}
                                {...col}
                                onPick={(v) => onChange(withTimePart(value, col.unit, v))}
                            />
                        ))}
                    </div>
                    <div className="ts-time-picker__footer">
                        <button
                            type="button"
                            className="ts-time-picker__action"
                            onClick={() => onChange(moment().format(TIME_FORMAT))}
                        >
                            Now
                        </button>
                        <Button variant="primary" size="sm" onClick={() => setOpen(false)}>
                            Apply
                        </Button>
                    </div>
                </div>
            )}
        </div>
    );
}
