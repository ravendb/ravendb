import React, { useEffect, useState } from "react";
import { Input, Label } from "reactstrap";

interface Duration {
    days: number;
    hours: number;
    minutes: number;
    seconds: number;
}

export interface DurationPickerProps {
    onChange: (totalSeconds: number) => void;
    totalSeconds: number;
    showDays?: boolean;
    showSeconds?: boolean;
    disabled?: boolean;
    placeholder?: {
        days?: string;
        hours?: string;
        minutes?: string;
        seconds?: string;
    };
}

export default function DurationPicker(props: DurationPickerProps) {
    const { totalSeconds, onChange, showDays, placeholder, showSeconds, disabled } = props;

    const initialValues = getInitialValues(totalSeconds, showDays);

    const [days, setDays] = useState(initialValues?.days ?? null);
    const [hours, setHours] = useState(initialValues?.hours ?? null);
    const [minutes, setMinutes] = useState(initialValues?.minutes ?? null);
    const [seconds, setSeconds] = useState(initialValues?.seconds ?? null);

    useEffect(() => {
        if (days == null && hours == null && minutes == null && seconds == null) {
            return;
        }

        const calculatedTotalSeconds = seconds + minutes * 60 + hours * 60 * 60 + days * 24 * 60 * 60;
        onChange(calculatedTotalSeconds);
    }, [onChange, days, hours, minutes, seconds, totalSeconds]);

    const getInputValue = (event: React.ChangeEvent<HTMLInputElement>) => {
        const value = event.currentTarget.value;
        return value === "" ? null : Number(value);
    };

    return (
        <div className="d-flex gap-1">
            {showDays && (
                <Label>
                    <span className="small-label">Days</span>
                    <Input
                        type="number"
                        min={0}
                        value={days}
                        placeholder={placeholder?.days}
                        onChange={(e) => setDays(getInputValue(e))}
                        disabled={disabled}
                    />
                </Label>
            )}
            <Label>
                <span className="small-label">Hours</span>
                <Input
                    type="number"
                    min={0}
                    value={hours}
                    placeholder={placeholder?.hours}
                    onChange={(e) => setHours(getInputValue(e))}
                    disabled={disabled}
                />
            </Label>
            <Label>
                <span className="small-label">Minutes</span>
                <Input
                    type="number"
                    min={0}
                    value={minutes}
                    placeholder={placeholder?.minutes}
                    onChange={(e) => setMinutes(getInputValue(e))}
                    disabled={disabled}
                />
            </Label>
            {showSeconds && (
                <Label>
                    <span className="small-label">Seconds</span>
                    <Input
                        type="number"
                        min={0}
                        value={seconds}
                        placeholder={placeholder?.seconds}
                        onChange={(e) => setSeconds(getInputValue(e))}
                        disabled={disabled}
                    />
                </Label>
            )}
        </div>
    );
}
function getInitialValues(totalSeconds: number, showDays: boolean): Duration {
    if (totalSeconds == null) {
        return null;
    }

    let total = totalSeconds,
        hours = 0,
        days = 0;

    const seconds = total % 60;
    total = Math.floor(total / 60);

    const minutes = total % 60;
    total = Math.floor(total / 60);

    if (showDays) {
        hours = total % 24;
        days = Math.floor(total / 24);
    } else {
        hours = total;
        days = 0;
    }

    return {
        days,
        hours,
        minutes,
        seconds,
    };
}
