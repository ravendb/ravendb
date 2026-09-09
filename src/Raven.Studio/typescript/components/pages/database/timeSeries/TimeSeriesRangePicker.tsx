import React, { useEffect, useMemo, useRef, useState } from "react";
import moment from "moment";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import DatePicker from "components/common/DatePicker";
import Select, { SelectOption } from "components/common/select/Select";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { InputItem } from "components/models/common";
import TimePicker, { TIME_FORMAT, TIME_PARSE_FORMATS } from "./TimePicker";
import RangeDatePickerHeader from "./RangeDatePickerHeader";
import { FilterTimezone, wallOf, zoneLabel, FULL_FORMAT } from "./timeSeriesRange.utils";
import "./TimeSeriesRangePicker.scss";

export type { FilterTimezone } from "./timeSeriesRange.utils";

type FilterMode = "between" | "before" | "after";

export interface TimeSeriesRangeState {
    range: filterTimeSeriesDates<moment.Moment | null>;
    canApply: boolean;
    timezone: FilterTimezone;
}

interface TimeSeriesRangePickerProps {
    startDate: moment.Moment | null;
    endDate: moment.Moment | null;
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
    range: (now: moment.Moment) => { start?: moment.Moment; end?: moment.Moment };
}

const presetsByMode: Record<FilterMode, PresetDef[]> = {
    between: [
        { label: "Today", range: (now) => ({ start: now.clone().startOf("day"), end: now }) },
        { label: "Last 3 days", range: (now) => ({ start: now.clone().subtract(3, "days"), end: now }) },
        { label: "Last 7 days", range: (now) => ({ start: now.clone().subtract(7, "days"), end: now }) },
        { label: "This week", range: (now) => ({ start: now.clone().startOf("week"), end: now }) },
        { label: "This month", range: (now) => ({ start: now.clone().startOf("month"), end: now }) },
        { label: "Last year", range: (now) => ({ start: now.clone().subtract(1, "year"), end: now }) },
    ],
    before: [
        { label: "Until Today", range: (now) => ({ end: now }) },
        { label: "Until 3 days ago", range: (now) => ({ end: now.clone().subtract(3, "days") }) },
        { label: "Until 7 days ago", range: (now) => ({ end: now.clone().subtract(7, "days") }) },
        { label: "Until 30 days ago", range: (now) => ({ end: now.clone().subtract(30, "days") }) },
        { label: "Until start of week", range: (now) => ({ end: now.clone().startOf("week") }) },
        { label: "Until start of month", range: (now) => ({ end: now.clone().startOf("month") }) },
        { label: "Until start of year", range: (now) => ({ end: now.clone().startOf("year") }) },
    ],
    after: [
        { label: "Since Today", range: (now) => ({ start: now.clone().startOf("day") }) },
        { label: "Since 3 days ago", range: (now) => ({ start: now.clone().subtract(3, "days") }) },
        { label: "Since 7 days ago", range: (now) => ({ start: now.clone().subtract(7, "days") }) },
        { label: "Since 30 days ago", range: (now) => ({ start: now.clone().subtract(30, "days") }) },
        { label: "Since start of week", range: (now) => ({ start: now.clone().startOf("week") }) },
        { label: "Since start of month", range: (now) => ({ start: now.clone().startOf("month") }) },
        { label: "Since start of year", range: (now) => ({ start: now.clone().startOf("year") }) },
    ],
};

function firstPresetFor(mode: FilterMode): PresetDef {
    return presetsByMode[mode][0];
}

const timezoneOptions: SelectOption<FilterTimezone>[] = [
    { value: "local", label: "Local" },
    { value: "utc", label: "UTC" },
];

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

function buildInstant(date: Date | null, time: string, tz: FilterTimezone): BuildResult {
    if (!date) {
        return { value: null, invalid: false };
    }
    const trimmed = time?.trim();
    const parsedTime = trimmed ? moment(trimmed, TIME_PARSE_FORMATS, true) : moment("00:00:00.000", TIME_FORMAT, true);
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

function oppositeHelper(instant: moment.Moment | null, tz: FilterTimezone): string | null {
    if (!instant) {
        return null;
    }
    return tz === "utc"
        ? instant.clone().local().format(FULL_FORMAT) + " (Local)"
        : instant.clone().utc().format(FULL_FORMAT) + "Z (UTC)";
}

interface SummarySegment {
    text: string;
    strong?: boolean;
}

function rangeSummary(
    mode: FilterMode,
    startValue: moment.Moment | null,
    endValue: moment.Moment | null,
    tz: FilterTimezone
): SummarySegment[] | null {
    const stamp = (m: moment.Moment): SummarySegment => ({ text: wallOf(m, tz).format(FULL_FORMAT), strong: true });
    const zone: SummarySegment = { text: ` (${zoneLabel(tz)}).` };

    if (mode === "between") {
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
    const [defaultNow] = useState(() => moment());

    const initialFields = useMemo(() => {
        if (startDate || endDate) {
            return { start: startDate ?? defaultNow, end: endDate ?? defaultNow, preset: null as string | null };
        }
        const today = firstPresetFor(initialMode(startDate, endDate));
        const todayRange = today.range(wallOf(defaultNow, initialTimezone));
        return { start: todayRange.start ?? defaultNow, end: todayRange.end ?? defaultNow, preset: today.label };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const [mode, setMode] = useState<FilterMode>(() => initialMode(startDate, endDate));
    const [tz, setTz] = useState<FilterTimezone>(initialTimezone);
    const [selectedPreset, setSelectedPreset] = useState<string | null>(initialFields.preset);

    const [startPickerDate, setStartPickerDate] = useState<Date | null>(() =>
        toPickerDate(initialFields.start, initialTimezone)
    );
    const [startTime, setStartTime] = useState<string>(() => toTimeText(initialFields.start, initialTimezone));
    const [endPickerDate, setEndPickerDate] = useState<Date | null>(() =>
        toPickerDate(initialFields.end, initialTimezone)
    );
    const [endTime, setEndTime] = useState<string>(() => toTimeText(initialFields.end, initialTimezone));

    // Whether a slot holds a real value (typed or prefilled) rather than the mode's "Today"
    // placeholder; a mode switch only overwrites still-untouched slots.
    const [touched, setTouched] = useState({ start: !!startDate, end: !!endDate });

    const startBuild = useMemo(() => buildInstant(startPickerDate, startTime, tz), [startPickerDate, startTime, tz]);
    const endBuild = useMemo(() => buildInstant(endPickerDate, endTime, tz), [endPickerDate, endTime, tz]);

    const changeTimezone = (next: FilterTimezone) => {
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
        const { start, end } = preset.range(wallOf(moment(), tz));
        if (start !== undefined) {
            setStartField(start);
            setTouched((prev) => ({ ...prev, start: true }));
        }
        if (end !== undefined) {
            setEndField(end);
            setTouched((prev) => ({ ...prev, end: true }));
        }
        setSelectedPreset(preset.label);
    };

    const changeMode = (next: FilterMode) => {
        setMode(next);
        const preset = firstPresetFor(next);
        const { start, end } = preset.range(wallOf(moment(), tz));
        if (start !== undefined && !touched.start) {
            setStartField(start);
        }
        if (end !== undefined && !touched.end) {
            setEndField(end);
        }
        const fullyDefaulted = (start === undefined || !touched.start) && (end === undefined || !touched.end);
        setSelectedPreset(fullyDefaulted ? preset.label : null);
    };

    const editStartDate = (d: Date | null) => {
        setStartPickerDate(d);
        setSelectedPreset(null);
        setTouched((prev) => ({ ...prev, start: true }));
    };
    const editStartTime = (t: string) => {
        setStartTime(t);
        setSelectedPreset(null);
        setTouched((prev) => ({ ...prev, start: true }));
    };
    const editEndDate = (d: Date | null) => {
        setEndPickerDate(d);
        setSelectedPreset(null);
        setTouched((prev) => ({ ...prev, end: true }));
    };
    const editEndTime = (t: string) => {
        setEndTime(t);
        setSelectedPreset(null);
        setTouched((prev) => ({ ...prev, end: true }));
    };

    const usesStart = mode === "between" || mode === "after";
    const usesEnd = mode === "between" || mode === "before";

    const rangeInvalid =
        mode === "between" && !!startBuild.value && !!endBuild.value && endBuild.value.isBefore(startBuild.value);

    const shownInvalid = (usesStart && startBuild.invalid) || (usesEnd && endBuild.invalid);
    // A slot the current mode uses must actually carry a date; a cleared field (null) would
    // otherwise become an open-ended bound and silently widen the range.
    const startMissing = usesStart && !startBuild.value;
    const endMissing = usesEnd && !endBuild.value;
    const canApply = !shownInvalid && !rangeInvalid && !startMissing && !endMissing;

    const startOut = usesStart ? startBuild.value : null;
    const endOut = usesEnd ? endBuild.value : null;
    const startMs = startOut ? startOut.valueOf() : null;
    const endMs = endOut ? endOut.valueOf() : null;

    // Emit only on real change; a ref-guard stops the effect looping on the fresh moments each render mints.
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

                {/* Both slots are always rendered; the unused one is hidden but kept in the layout
                    (order:1 in the SCSS drops it to the bottom) so the modal height never changes. */}
                <div className="vstack gap-2">
                    <DateTimeField
                        label="Start date"
                        hidden={!usesStart}
                        date={startPickerDate}
                        time={startTime}
                        timeInvalid={startBuild.invalid}
                        timezone={tz}
                        helper={oppositeHelper(startBuild.value, tz)}
                        onDateChange={editStartDate}
                        onTimeChange={editStartTime}
                    />
                    <DateTimeField
                        label="End date"
                        hidden={!usesEnd}
                        date={endPickerDate}
                        time={endTime}
                        timeInvalid={endBuild.invalid}
                        timezone={tz}
                        helper={oppositeHelper(endBuild.value, tz)}
                        onDateChange={editEndDate}
                        onTimeChange={editEndTime}
                    />
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
    hidden?: boolean;
    date: Date | null;
    time: string;
    timeInvalid: boolean;
    timezone: FilterTimezone;
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
    timezone,
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
                            dateFormat="yyyy-MM-dd"
                            calendarClassName="ts-range-datepicker"
                            popperClassName="ts-range-datepicker-popper"
                            renderCustomHeader={(headerProps) => (
                                <RangeDatePickerHeader
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
                    <TimePicker value={time} invalid={timeInvalid} timezone={timezone} onChange={onTimeChange} />
                </div>
            </div>
            <div className="text-muted small mt-1" style={{ minHeight: "1.2em" }}>
                {helper}
            </div>
        </div>
    );
}
