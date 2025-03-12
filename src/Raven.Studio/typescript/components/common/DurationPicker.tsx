import React, { ChangeEvent, useEffect, useState } from "react";
import Form from "react-bootstrap/Form";

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
}

export default function DurationPicker(props: DurationPickerProps) {
    const { totalSeconds, onChange, showDays, showSeconds, disabled } = props;

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
                <div>
                    <Form.Label htmlFor="days" className="small-label">
                        Days
                    </Form.Label>
                    <Form.Control
                        type="number"
                        id="days"
                        min={0}
                        value={days}
                        onChange={(e: ChangeEvent<HTMLInputElement>) => setDays(getInputValue(e))}
                        disabled={disabled}
                    />
                </div>
            )}
            <div>
                <Form.Label htmlFor="hours" className="small-label">
                    Hours
                </Form.Label>
                <Form.Control
                    type="number"
                    min={0}
                    id="hours"
                    value={hours}
                    onChange={(e: ChangeEvent<HTMLInputElement>) => setHours(getInputValue(e))}
                    disabled={disabled}
                />
            </div>
            <div>
                <Form.Label htmlFor="minutes" className="small-label">
                    Minutes
                </Form.Label>
                <Form.Control
                    type="number"
                    id="minutes"
                    min={0}
                    value={minutes}
                    onChange={(e: ChangeEvent<HTMLInputElement>) => setMinutes(getInputValue(e))}
                    disabled={disabled}
                />
            </div>
            {showSeconds && (
                <div>
                    <Form.Label htmlFor="seconds" className="small-label">
                        Seconds
                    </Form.Label>
                    <Form.Control
                        type="number"
                        id="seconds"
                        min={0}
                        value={seconds}
                        onChange={(e: ChangeEvent<HTMLInputElement>) => setSeconds(getInputValue(e))}
                        disabled={disabled}
                    />
                </div>
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
