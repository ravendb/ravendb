import React, { useEffect, useState } from "react";
import { Input, Label } from "reactstrap";

interface Duration {
    days: number;
    hours: number;
    minutes: number;
    seconds: number;
}

interface DurationPickerProps {
    onChange: (totalSeconds: number) => void;
    totalSeconds: number;
    showDays?: boolean;
    showSeconds?: boolean;
}

export default function DurationPicker(props: DurationPickerProps) {
    const { totalSeconds, onChange, showDays, showSeconds } = props;

    const initialValues = getInitialValues(totalSeconds, showDays);

    const [days, setDays] = useState(initialValues.days);
    const [hours, setHours] = useState(initialValues.hours);
    const [minutes, setMinutes] = useState(initialValues.minutes);
    const [seconds, setSeconds] = useState(initialValues.seconds);

    useEffect(() => {
        const totalSeconds = seconds + minutes * 60 + hours * 60 * 60 + days * 24 * 60 * 60;
        onChange(totalSeconds);
    }, [onChange, days, hours, minutes, seconds]);

    const getInputValue = (event: React.ChangeEvent<HTMLInputElement>) => {
        const value = event.currentTarget.value;
        return value === "" ? null : Number(value);
    };

    return (
        <div className="d-flex gap-1">
            {showDays && (
                <Label>
                    <span className="small-label">Days</span>
                    <Input type="number" min={0} value={days} onChange={(e) => setDays(getInputValue(e))} />
                </Label>
            )}
            <Label>
                <span className="small-label">Hours</span>
                <Input type="number" min={0} value={hours} onChange={(e) => setHours(getInputValue(e))} />
            </Label>
            <Label>
                <span className="small-label">Minutes</span>
                <Input type="number" min={0} value={minutes} onChange={(e) => setMinutes(getInputValue(e))} />
            </Label>
            {showSeconds && (
                <Label>
                    <span className="small-label">Seconds</span>
                    <Input type="number" min={0} value={seconds} onChange={(e) => setSeconds(getInputValue(e))} />
                </Label>
            )}
        </div>
    );
}
function getInitialValues(totalSeconds: number, showDays: boolean): Duration {
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
