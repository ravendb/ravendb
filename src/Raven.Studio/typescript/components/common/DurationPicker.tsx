import classNames from "classnames";
import React, { ChangeEvent, useEffect, useState } from "react";
import Form from "react-bootstrap/Form";
import FormGroup from "react-bootstrap/FormGroup";
import FormLabel from "react-bootstrap/FormLabel";

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
    isFlexGrow?: boolean;
}

export default function DurationPicker(props: DurationPickerProps) {
    const { totalSeconds, onChange, showDays, placeholder, showSeconds, disabled, isFlexGrow } = props;

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
        <div className={classNames("d-flex gap-1", { "flex-grow-1": isFlexGrow })}>
            {showDays && (
                <FormGroup controlId="days" className={classNames({ "flex-grow-1": isFlexGrow })}>
                    <FormLabel className="small-label">Days</FormLabel>
                    <Form.Control
                        type="number"
                        min={0}
                        value={days ?? ""}
                        placeholder={placeholder?.days}
                        onChange={(e: ChangeEvent<HTMLInputElement>) => setDays(getInputValue(e))}
                        disabled={disabled}
                    />
                </FormGroup>
            )}
            <FormGroup controlId="hours" className={classNames({ "flex-grow-1": isFlexGrow })}>
                <FormLabel className="small-label">Hours</FormLabel>
                <Form.Control
                    type="number"
                    min={0}
                    value={hours ?? ""}
                    placeholder={placeholder?.hours}
                    onChange={(e: ChangeEvent<HTMLInputElement>) => setHours(getInputValue(e))}
                    disabled={disabled}
                />
            </FormGroup>
            <FormGroup controlId="minutes" className={classNames({ "flex-grow-1": isFlexGrow })}>
                <FormLabel className="small-label">Minutes</FormLabel>
                <Form.Control
                    type="number"
                    min={0}
                    value={minutes ?? ""}
                    placeholder={placeholder?.minutes}
                    onChange={(e: ChangeEvent<HTMLInputElement>) => setMinutes(getInputValue(e))}
                    disabled={disabled}
                />
            </FormGroup>
            {showSeconds && (
                <FormGroup controlId="seconds" className={classNames({ "flex-grow-1": isFlexGrow })}>
                    <FormLabel className="small-label">Seconds</FormLabel>
                    <Form.Control
                        type="number"
                        min={0}
                        value={seconds ?? ""}
                        placeholder={placeholder?.seconds}
                        onChange={(e: ChangeEvent<HTMLInputElement>) => setSeconds(getInputValue(e))}
                        disabled={disabled}
                    />
                </FormGroup>
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
