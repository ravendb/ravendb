import React, { useEffect, useMemo, useRef, useState } from "react";
import moment from "moment";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import DatePicker from "components/common/DatePicker";
import Select, { SelectOption } from "components/common/select/Select";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { InputItem } from "components/models/common";
import "./TimeSeriesRangePicker.scss";

const TIME_FORMAT = "HH:mm:ss.SSS";
const FULL_FORMAT = "YYYY-MM-DD HH:mm:ss.SSS";

type FilterMode = "between" | "before" | "after";
export type FilterTimezone = "local" | "utc";

// The resolved range this picker produces. Matches filterTimeSeriesDates<moment.Moment>.
export interface TimeSeriesRange {
    startDate: moment.Moment | null;
    endDate: moment.Moment | null;
}

export interface TimeSeriesRangeState {
    range: TimeSeriesRange;
    canApply: boolean;
    // Zone the user is currently working in. Reported back so the caller can keep
    // the time series grid's own time zone setting in sync with the filter.
    timezone: FilterTimezone;
}

interface TimeSeriesRangePickerProps {
    startDate: moment.Moment | null;
    endDate: moment.Moment | null;
    // Zone the picker opens in. The time series grid owns this setting, so the
    // filter always starts out speaking the same zone the timestamps are shown in.
    // Changing it inside the picker is local to the picker and does not affect the grid.
    initialTimezone?: FilterTimezone;
    onChange: (state: TimeSeriesRangeState) => void;
}

const modeItems: InputItem<FilterMode>[] = [
    { label: "Between", value: "between" },
    { label: "Before", value: "before" },
    { label: "After", value: "after" },
];

interface PresetDef {
    label: string;
    range: () => { start?: moment.Moment; end?: moment.Moment };
}

const presetsByMode: Record<FilterMode, PresetDef[]> = {
    between: [
        { label: "Today", range: () => ({ start: moment().startOf("day"), end: moment() }) },
        { label: "Last 3 days", range: () => ({ start: moment().subtract(3, "days"), end: moment() }) },
        { label: "Last 7 days", range: () => ({ start: moment().subtract(7, "days"), end: moment() }) },
        { label: "This week", range: () => ({ start: moment().startOf("week"), end: moment() }) },
        { label: "This month", range: () => ({ start: moment().startOf("month"), end: moment() }) },
        { label: "Last year", range: () => ({ start: moment().subtract(1, "year"), end: moment() }) },
    ],
    before: [
        { label: "Until Today", range: () => ({ end: moment() }) },
        { label: "Until 3 days ago", range: () => ({ end: moment().subtract(3, "days") }) },
        { label: "Until 7 days ago", range: () => ({ end: moment().subtract(7, "days") }) },
        { label: "Until 30 days ago", range: () => ({ end: moment().subtract(30, "days") }) },
        { label: "Until start of week", range: () => ({ end: moment().startOf("week") }) },
        { label: "Until start of month", range: () => ({ end: moment().startOf("month") }) },
        { label: "Until start of year", range: () => ({ end: moment().startOf("year") }) },
    ],
    after: [
        { label: "Since Today", range: () => ({ start: moment().startOf("day") }) },
        { label: "Since 3 days ago", range: () => ({ start: moment().subtract(3, "days") }) },
        { label: "Since 7 days ago", range: () => ({ start: moment().subtract(7, "days") }) },
        { label: "Since 30 days ago", range: () => ({ start: moment().subtract(30, "days") }) },
        { label: "Since start of week", range: () => ({ start: moment().startOf("week") }) },
        { label: "Since start of month", range: () => ({ start: moment().startOf("month") }) },
        { label: "Since start of year", range: () => ({ start: moment().startOf("year") }) },
    ],
};

// Each mode's first preset is its "current day" shortcut (Today / Until Today / Since Today),
// used as the default selection when the dialog opens without an incoming range.
function firstPresetFor(mode: FilterMode): PresetDef {
    return presetsByMode[mode][0];
}

const timezoneOptions: SelectOption<FilterTimezone>[] = [
    { value: "local", label: "Local" },
    { value: "utc", label: "UTC" },
];

// Wall-clock representation of an instant, seen from the selected timezone.
function wallOf(instant: moment.Moment, tz: FilterTimezone): moment.Moment {
    return tz === "utc" ? instant.clone().utc() : instant.clone().local();
}

// A browser-local Date whose local components equal the timezone wall-clock, so the
// date picker (which always works in browser-local time) shows the right calendar day.
function toPickerDate(instant: moment.Moment | null, tz: FilterTimezone): Date | null {
    if (!instant) {
        return null;
    }
    const w = wallOf(instant, tz);
    return new Date(w.year(), w.month(), w.date());
}

function toTimeText(instant: moment.Moment | null, tz: FilterTimezone): string {
    return instant ? wallOf(instant, tz).format(TIME_FORMAT) : "";
}

interface BuildResult {
    value: moment.Moment | null;
    invalid: boolean;
}

// Combine the picked day + typed time, interpreted in the selected timezone, into an instant.
function buildInstant(date: Date | null, time: string, tz: FilterTimezone): BuildResult {
    if (!date) {
        return { value: null, invalid: false };
    }
    const trimmed = time?.trim();
    const parsedTime = trimmed ? moment(trimmed, TIME_FORMAT, true) : moment("00:00:00.000", TIME_FORMAT, true);
    if (trimmed && !parsedTime.isValid()) {
        return { value: null, invalid: true };
    }
    const parts: [number, number, number, number, number, number, number] = [
        date.getFullYear(),
        date.getMonth(),
        date.getDate(),
        parsedTime.hour(),
        parsedTime.minute(),
        parsedTime.second(),
        parsedTime.millisecond(),
    ];
    return { value: tz === "utc" ? moment.utc(parts) : moment(parts), invalid: false };
}

// Helper line showing the same instant expressed in the opposite timezone.
function oppositeHelper(instant: moment.Moment | null, tz: FilterTimezone): string | null {
    if (!instant) {
        return null;
    }
    return tz === "utc"
        ? instant.clone().local().format(FULL_FORMAT) + " (Local)"
        : instant.clone().utc().format(FULL_FORMAT) + "Z (UTC)";
}

// Wall-clock timestamp of an instant in the selected timezone (no zone tag — the sidebar
// dropdown already states the zone globally).
function wallStamp(instant: moment.Moment, tz: FilterTimezone): string {
    return wallOf(instant, tz).format(FULL_FORMAT);
}

// A piece of the summary line. Only the timestamp is emphasised (`strong`); the words around it
// stay regular weight.
interface SummarySegment {
    text: string;
    strong?: boolean;
}

// A one-line description of what the current selection filters, worded to match the Between /
// Before / After tabs. Stated in terms of the chosen bound(s) alone, so it's always correct
// regardless of where the data actually sits.
function rangeSummary(
    mode: FilterMode,
    startValue: moment.Moment | null,
    endValue: moment.Moment | null,
    tz: FilterTimezone
): SummarySegment[] | null {
    const stamp = (m: moment.Moment): SummarySegment => ({ text: wallStamp(m, tz), strong: true });
    // Zone suffix so the reader knows whether the shown timestamps are Local or UTC.
    const zone: SummarySegment = { text: ` (${tz === "utc" ? "UTC" : "Local"}).` };

    if (mode === "between") {
        // Skip while the range is inverted — the "end must be after start" error covers that.
        if (!startValue || !endValue || endValue.isBefore(startValue)) {
            return null;
        }
        return [{ text: "Includes all entries between " }, stamp(startValue), { text: " and " }, stamp(endValue), zone];
    }
    if (mode === "before") {
        return endValue ? [{ text: "Includes all entries before " }, stamp(endValue), zone] : null;
    }
    if (mode === "after") {
        return startValue ? [{ text: "Includes all entries after " }, stamp(startValue), zone] : null;
    }
    return null;
}

function initialMode(startDate: moment.Moment | null, endDate: moment.Moment | null): FilterMode {
    if (startDate && !endDate) {
        return "after";
    }
    if (!startDate && endDate) {
        return "before";
    }
    return "between";
}

export default function TimeSeriesRangePicker({
    startDate,
    endDate,
    initialTimezone = "local",
    onChange,
}: TimeSeriesRangePickerProps) {
    // Shared "now" used to default any field that has no incoming filter value.
    const [defaultNow] = useState(() => moment());

    // With no incoming filter range, open on the mode's "Today" preset so each option starts on
    // a sensible current-day window. An incoming range (e.g. prefilled from an active filter)
    // wins instead, and shows as a custom selection.
    const initialFields = useMemo(() => {
        if (startDate || endDate) {
            return { start: startDate ?? defaultNow, end: endDate ?? defaultNow, preset: null as string | null };
        }
        const today = firstPresetFor(initialMode(startDate, endDate));
        const todayRange = today.range();
        return { start: todayRange.start ?? defaultNow, end: todayRange.end ?? defaultNow, preset: today.label };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const [mode, setMode] = useState<FilterMode>(() => initialMode(startDate, endDate));
    const [tz, setTz] = useState<FilterTimezone>(initialTimezone);
    // Label of the currently applied preset shortcut, highlighted in the sidebar.
    // Cleared whenever the user edits a date/time, so it never lies.
    const [selectedPreset, setSelectedPreset] = useState<string | null>(initialFields.preset);

    // "start" slot drives Between top field and the After single field.
    // "end" slot drives Between bottom field and the Before single field.
    const [startPickerDate, setStartPickerDate] = useState<Date | null>(() =>
        toPickerDate(initialFields.start, initialTimezone)
    );
    const [startTime, setStartTime] = useState<string>(() => toTimeText(initialFields.start, initialTimezone));
    const [endPickerDate, setEndPickerDate] = useState<Date | null>(() =>
        toPickerDate(initialFields.end, initialTimezone)
    );
    const [endTime, setEndTime] = useState<string>(() => toTimeText(initialFields.end, initialTimezone));

    const startBuild = useMemo(() => buildInstant(startPickerDate, startTime, tz), [startPickerDate, startTime, tz]);
    const endBuild = useMemo(() => buildInstant(endPickerDate, endTime, tz), [endPickerDate, endTime, tz]);

    const changeTimezone = (next: FilterTimezone) => {
        // Preserve the instants, re-express the visible wall-clock in the new timezone.
        if (startBuild.value) {
            setStartPickerDate(toPickerDate(startBuild.value, next));
            setStartTime(toTimeText(startBuild.value, next));
        }
        if (endBuild.value) {
            setEndPickerDate(toPickerDate(endBuild.value, next));
            setEndTime(toTimeText(endBuild.value, next));
        }
        setTz(next);
    };

    const setStartField = (instant: moment.Moment | null) => {
        setStartPickerDate(toPickerDate(instant, tz));
        setStartTime(toTimeText(instant, tz));
    };
    const setEndField = (instant: moment.Moment | null) => {
        setEndPickerDate(toPickerDate(instant, tz));
        setEndTime(toTimeText(instant, tz));
    };

    const applyPreset = (preset: PresetDef) => {
        const { start, end } = preset.range();
        if (start !== undefined) {
            setStartField(start);
        }
        if (end !== undefined) {
            setEndField(end);
        }
        setSelectedPreset(preset.label);
    };

    const changeMode = (next: FilterMode) => {
        setMode(next);
        // Reset the newly-selected option to its "Today" preset by default.
        applyPreset(firstPresetFor(next));
    };

    // Manual edits to the fields invalidate the active preset highlight.
    const editStartDate = (d: Date | null) => {
        setStartPickerDate(d);
        setSelectedPreset(null);
    };
    const editStartTime = (t: string) => {
        setStartTime(t);
        setSelectedPreset(null);
    };
    const editEndDate = (d: Date | null) => {
        setEndPickerDate(d);
        setSelectedPreset(null);
    };
    const editEndTime = (t: string) => {
        setEndTime(t);
        setSelectedPreset(null);
    };

    const usesStart = mode === "between" || mode === "after";
    const usesEnd = mode === "between" || mode === "before";

    const rangeInvalid =
        mode === "between" && !!startBuild.value && !!endBuild.value && endBuild.value.isBefore(startBuild.value);

    const shownInvalid = (usesStart && startBuild.invalid) || (usesEnd && endBuild.invalid);
    const canApply = !shownInvalid && !rangeInvalid;

    const startOut = usesStart ? startBuild.value : null;
    const endOut = usesEnd ? endBuild.value : null;
    const startMs = startOut ? startOut.valueOf() : null;
    const endMs = endOut ? endOut.valueOf() : null;

    // Emit the resolved range to the parent only when it actually changes. A ref-guard keeps
    // this from re-firing on every render (each render mints fresh moment instances), which
    // would otherwise loop against the parent's setState.
    const onChangeRef = useRef(onChange);
    onChangeRef.current = onChange;
    const lastSig = useRef<string | null>(null);
    useEffect(() => {
        const sig = `${startMs}|${endMs}|${canApply}|${tz}`;
        if (lastSig.current === sig) {
            return;
        }
        lastSig.current = sig;
        onChangeRef.current({ range: { startDate: startOut, endDate: endOut }, canApply, timezone: tz });
    });

    return (
        <div className="d-flex gap-4 ts-range-picker__body">
            <div className="ts-range-picker__sidebar vstack">
                <div className="vstack mb-3">
                    {presetsByMode[mode].map((preset) => {
                        const isActive = selectedPreset === preset.label;
                        return (
                            <Button
                                key={preset.label}
                                variant="link"
                                size="sm"
                                className={classNames(
                                    "ts-range-picker__preset d-flex align-items-center justify-content-between text-decoration-none",
                                    { "ts-range-picker__preset--active": isActive }
                                )}
                                onClick={() => applyPreset(preset)}
                            >
                                <span>{preset.label}</span>
                            </Button>
                        );
                    })}
                </div>
                <div className="mt-auto">
                    <label className="md-label d-block mb-1">Time zone</label>
                    <Select
                        options={timezoneOptions}
                        value={timezoneOptions.find((o) => o.value === tz)}
                        onChange={(opt) => opt && changeTimezone(opt.value)}
                    />
                </div>
            </div>

            <div className="vr" />

            <div className="vstack gap-3 flex-grow-1">
                <MultiRadioToggle<FilterMode>
                    className="ts-range-picker__mode"
                    inputItems={modeItems}
                    selectedItem={mode}
                    setSelectedItem={(x) => changeMode(x)}
                />

                {/* Start and End are one unit - grouped so they sit closer to each other than
                    to the mode toggle above and the summary below.

                    Both slots are ALWAYS rendered, and always labelled, so every mode occupies
                    exactly the same vertical space. Before/After only use one of them; the unused
                    slot stays in the layout but is hidden (see `--placeholder` in the SCSS). This
                    keeps the modal height identical across modes by construction, rather than by
                    a hand-tuned min-height that goes stale whenever this block changes. */}
                <div className="vstack gap-2">
                    {[
                        <DateTimeField
                            key="start"
                            label="Start date"
                            hidden={!usesStart}
                            date={startPickerDate}
                            time={startTime}
                            timeInvalid={startBuild.invalid}
                            helper={oppositeHelper(startBuild.value, tz)}
                            onDateChange={editStartDate}
                            onTimeChange={editStartTime}
                        />,
                        <DateTimeField
                            key="end"
                            label="End date"
                            hidden={!usesEnd}
                            date={endPickerDate}
                            time={endTime}
                            timeInvalid={endBuild.invalid}
                            helper={oppositeHelper(endBuild.value, tz)}
                            onDateChange={editEndDate}
                            onTimeChange={editEndTime}
                        />,
                        // Visible slot first, reserved slot last. Before/After use only one field,
                        // and a blank slot left in the middle reads as a rendering bug - trailing
                        // slack just reads as padding. Keys keep React from remounting the pickers
                        // (and losing focus) when the order flips on a mode change.
                    ].sort((a, b) => Number(a.props.hidden) - Number(b.props.hidden))}
                </div>

                {rangeInvalid && (
                    <div className="text-danger small">End date must be greater than (or equal to) start date.</div>
                )}

                {(() => {
                    const summary = rangeSummary(mode, startBuild.value, endBuild.value, tz);
                    if (!summary) {
                        return null;
                    }
                    return (
                        <div className="ts-range-picker__summary-slot">
                            <div className="ts-range-picker__summary hstack gap-2 align-items-center">
                                <Icon icon="info" color="info" margin="m-0" />
                                <span>
                                    {summary.map((seg, i) =>
                                        seg.strong ? (
                                            <strong key={i}>{seg.text}</strong>
                                        ) : (
                                            <span key={i}>{seg.text}</span>
                                        )
                                    )}
                                </span>
                            </div>
                        </div>
                    );
                })()}
            </div>
        </div>
    );
}

interface DateTimeFieldProps {
    label?: string;
    // Keeps the slot in the layout but invisible and non-interactive, so the modal
    // height doesn't change between Between (two slots) and Before/After (one).
    hidden?: boolean;
    date: Date | null;
    time: string;
    timeInvalid: boolean;
    helper: string | null;
    onDateChange: (date: Date | null) => void;
    onTimeChange: (time: string) => void;
}

function DateTimeField({
    label,
    hidden,
    date,
    time,
    timeInvalid,
    helper,
    onDateChange,
    onTimeChange,
}: DateTimeFieldProps) {
    return (
        <div
            className={classNames({ "mt-1": !!label, "ts-range-picker__field--placeholder": hidden })}
            aria-hidden={hidden}
        >
            {label && <div className="fw-bold mb-2">{label}</div>}
            <div className="d-flex gap-2">
                <div className="flex-grow-1">
                    <label className="md-label d-block mb-1">Date</label>
                    <div className="ts-range-picker__input-icon">
                        <Icon icon="calendar" margin="m-0" className="ts-range-picker__input-icon-glyph" />
                        <DatePicker
                            selected={date}
                            onChange={onDateChange}
                            dateFormat="dd/MM/yyyy"
                            calendarClassName="ts-range-datepicker"
                            popperClassName="ts-range-datepicker-popper"
                            renderCustomHeader={(headerProps) => (
                                <DatePickerHeader
                                    date={headerProps.date}
                                    changeMonth={headerProps.changeMonth}
                                    changeYear={headerProps.changeYear}
                                    decreaseMonth={headerProps.decreaseMonth}
                                    increaseMonth={headerProps.increaseMonth}
                                    prevMonthButtonDisabled={headerProps.prevMonthButtonDisabled}
                                    nextMonthButtonDisabled={headerProps.nextMonthButtonDisabled}
                                />
                            )}
                        />
                    </div>
                </div>
                <div className="flex-grow-1">
                    <label className="md-label d-block mb-1">Time</label>
                    <TimePicker value={time} invalid={timeInvalid} onChange={onTimeChange} />
                </div>
            </div>
            <div className="text-muted small mt-1" style={{ minHeight: "1.2em" }}>
                {helper}
            </div>
        </div>
    );
}

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
    const parsed = moment(trimmed, ["HH:mm:ss.SSS", "HH:mm:ss", "HH:mm"], true);
    if (!parsed.isValid()) {
        return null;
    }
    return { h: parsed.hour(), m: parsed.minute(), s: parsed.second(), ms: parsed.millisecond() };
}

// Replace a single component (hour/minute/second) while preserving the rest — crucially the
// millisecond part the columns don't expose, so clicking a column never discards typed ms.
function withTimePart(text: string, unit: keyof TimeParts, value: number): string {
    const parts = parseTimeParts(text) ?? { h: 0, m: 0, s: 0, ms: 0 };
    parts[unit] = value;
    return `${pad2(parts.h)}:${pad2(parts.m)}:${pad2(parts.s)}.${String(parts.ms).padStart(3, "0")}`;
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
// reason as HeaderSelect above.
function TimePicker({ value, invalid, onChange }: TimePickerProps) {
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

const MONTH_NAMES = moment.months();

const monthOptions: SelectOption<number>[] = MONTH_NAMES.map((name, i) => ({ label: name, value: i }));

interface HeaderSelectProps {
    ariaLabel: string;
    value: number;
    options: SelectOption<number>[];
    onChange: (value: number) => void;
    className?: string;
}

// A controlled dropdown (button + list) styled to mirror Studio's react-select. Used instead of the
// real Select / a native <select> because both misbehave inside react-datepicker's calendar (its
// focus and outside-click handling fights them). This stays fully within our control.
function HeaderSelect({ ariaLabel, value, options, onChange, className }: HeaderSelectProps) {
    const [open, setOpen] = useState(false);
    const ref = useRef<HTMLDivElement>(null);
    const menuRef = useRef<HTMLUListElement>(null);
    const activeRef = useRef<HTMLButtonElement>(null);

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

    // Scroll the currently-selected option into the middle when the menu opens, so reopening a
    // dropdown always shows the active value without scrolling to find it (matches TimeColumn).
    useEffect(() => {
        if (!open) {
            return;
        }
        const menu = menuRef.current;
        const active = activeRef.current;
        if (menu && active) {
            menu.scrollTop = active.offsetTop - menu.clientHeight / 2 + active.clientHeight / 2;
        }
    }, [open]);

    const selected = options.find((o) => o.value === value);

    return (
        <div className={classNames("ts-dp-header__select", className)} ref={ref}>
            <button
                type="button"
                className="ts-dp-header__control"
                onClick={() => setOpen((o) => !o)}
                aria-label={ariaLabel}
                aria-expanded={open}
            >
                <span className="ts-dp-header__value">{selected?.label}</span>
                <Icon icon={open ? "chevron-up" : "chevron-down"} margin="m-0" className="ts-dp-header__caret" />
            </button>
            {open && (
                <ul className="ts-dp-header__menu" ref={menuRef}>
                    {options.map((o) => (
                        <li key={o.value}>
                            <button
                                type="button"
                                ref={o.value === value ? activeRef : undefined}
                                className={classNames("ts-dp-header__option", {
                                    "ts-dp-header__option--active": o.value === value,
                                })}
                                onClick={() => {
                                    onChange(o.value);
                                    setOpen(false);
                                }}
                            >
                                {o.label}
                            </button>
                        </li>
                    ))}
                </ul>
            )}
        </div>
    );
}

interface DatePickerHeaderProps {
    date: Date;
    changeMonth: (month: number) => void;
    changeYear: (year: number) => void;
    decreaseMonth: () => void;
    increaseMonth: () => void;
    prevMonthButtonDisabled: boolean;
    nextMonthButtonDisabled: boolean;
}

// Custom calendar header laying out prev/next arrows and two separate month/year select dropdowns
// in a single flex row. Done as a custom header because react-datepicker's built-in arrows are
// absolutely positioned and overlap the dropdowns once the redundant month label is removed.
function DatePickerHeader({
    date,
    changeMonth,
    changeYear,
    decreaseMonth,
    increaseMonth,
    prevMonthButtonDisabled,
    nextMonthButtonDisabled,
}: DatePickerHeaderProps) {
    const currentMonth = date.getMonth();
    const currentYear = date.getFullYear();

    const yearOptions = useMemo<SelectOption<number>[]>(() => {
        const thisYear = new Date().getFullYear();
        const start = Math.min(thisYear - 20, currentYear - 5);
        const end = Math.max(thisYear + 5, currentYear + 5);
        const list: SelectOption<number>[] = [];
        for (let y = start; y <= end; y++) {
            list.push({ label: String(y), value: y });
        }
        return list;
    }, [currentYear]);

    return (
        <div className="ts-dp-header">
            <button
                type="button"
                className="ts-dp-header__nav"
                onClick={decreaseMonth}
                disabled={prevMonthButtonDisabled}
                aria-label="Previous month"
            >
                <Icon icon="chevron-left" margin="m-0" />
            </button>
            <HeaderSelect
                ariaLabel="Month"
                className="ts-dp-header__select--month"
                value={currentMonth}
                options={monthOptions}
                onChange={changeMonth}
            />
            <HeaderSelect
                ariaLabel="Year"
                className="ts-dp-header__select--year"
                value={currentYear}
                options={yearOptions}
                onChange={changeYear}
            />
            <button
                type="button"
                className="ts-dp-header__nav"
                onClick={increaseMonth}
                disabled={nextMonthButtonDisabled}
                aria-label="Next month"
            >
                <Icon icon="chevron-right" margin="m-0" />
            </button>
        </div>
    );
}
