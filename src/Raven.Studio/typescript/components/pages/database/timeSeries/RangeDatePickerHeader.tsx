import React, { useEffect, useMemo, useRef, useState } from "react";
import moment from "moment";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { SelectOption } from "components/common/select/Select";

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
export default function RangeDatePickerHeader({
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
