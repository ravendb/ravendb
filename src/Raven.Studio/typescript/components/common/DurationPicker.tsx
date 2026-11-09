import classNames from "classnames";
import React, { ChangeEvent, useEffect, useRef, useState } from "react";
import Form from "react-bootstrap/Form";
import FormGroup from "react-bootstrap/FormGroup";
import InputGroup from "react-bootstrap/InputGroup";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import useUniqueId from "components/hooks/useUniqueId";

interface Duration {
    days: number;
    hours: number;
    minutes: number;
    seconds: number;
}

type DurationPart = keyof Duration;

type DurationPlaceholder = Partial<Record<DurationPart, string>>;

export interface DurationPickerProps {
    onChange: (totalSeconds: number) => void;
    totalSeconds: number;
    showDays?: boolean;
    showSeconds?: boolean;
    disabled?: boolean;
    placeholder?: DurationPlaceholder;
    defaultTotalSeconds?: number;
    isFlexGrow?: boolean;
    "aria-labelledby"?: string;
    onInvalidPartMessageChange?: (message: string) => void;
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
        showDays,
        placeholder,
        defaultTotalSeconds,
        showSeconds,
        disabled,
        isFlexGrow,
        "aria-labelledby": ariaLabelledBy,
        onInvalidPartMessageChange,
    } = props;

    const t = useStudioTranslation("durationPicker");
    const commonT = useStudioTranslation("common");
    const controlIdPrefix = useUniqueId("duration-picker-");
    const initialValues = getInitialValues(totalSeconds, showDays);

    const [days, setDays] = useState(initialValues?.days ?? null);
    const [hours, setHours] = useState(initialValues?.hours ?? null);
    const [minutes, setMinutes] = useState(initialValues?.minutes ?? null);
    const [seconds, setSeconds] = useState(initialValues?.seconds ?? null);

    // the last value this picker and its consumer agreed on, so that emitting and syncing don't echo each other
    const lastTotalSecondsRef = useRef(totalSeconds ?? null);

    const parts: { name: DurationPart; value: number; setValue: (value: number) => void }[] = [
        { name: "days" as const, value: days, setValue: setDays, isVisible: showDays },
        { name: "hours" as const, value: hours, setValue: setHours, isVisible: true },
        { name: "minutes" as const, value: minutes, setValue: setMinutes, isVisible: true },
        { name: "seconds" as const, value: seconds, setValue: setSeconds, isVisible: showSeconds },
    ].filter((part) => part.isVisible);

    const isEmpty = parts.every((part) => part.value == null);
    const invalidPartMessageKey = getInvalidPartMessageKey(parts.map((part) => part.value));
    const invalidPartMessage = invalidPartMessageKey ? t(invalidPartMessageKey) : null;
    const hasInvalidPart = invalidPartMessageKey != null;

    useEffect(() => {
        onInvalidPartMessageChange?.(invalidPartMessage);
    }, [onInvalidPartMessageChange, invalidPartMessage]);

    useEffect(() => {
        const calculatedTotalSeconds = isEmpty
            ? null
            : hasInvalidPart
              ? NaN
              : seconds + minutes * 60 + hours * 60 * 60 + days * 24 * 60 * 60;

        if (Object.is(calculatedTotalSeconds, lastTotalSecondsRef.current)) {
            return;
        }

        lastTotalSecondsRef.current = calculatedTotalSeconds;
        onChange(calculatedTotalSeconds);
    }, [onChange, days, hours, minutes, seconds, isEmpty, hasInvalidPart]);

    useEffect(() => {
        const consumerTotalSeconds = totalSeconds ?? null;
        if (Object.is(consumerTotalSeconds, lastTotalSecondsRef.current)) {
            return;
        }

        lastTotalSecondsRef.current = consumerTotalSeconds;

        const values = getInitialValues(totalSeconds, showDays);
        setDays(values?.days ?? null);
        setHours(values?.hours ?? null);
        setMinutes(values?.minutes ?? null);
        setSeconds(values?.seconds ?? null);
    }, [totalSeconds, showDays]);

    const getPlaceholder = (part: DurationPart): string => {
        if (defaultTotalSeconds == null) {
            return placeholder?.[part];
        }
        if (!isEmpty) {
            return ZERO_DURATION_PLACEHOLDER[part];
        }
        return commonT("defaultPlaceholder", { value: getInitialValues(defaultTotalSeconds, showDays)[part] });
    };

    const getInputValue = (event: React.ChangeEvent<HTMLInputElement>) => {
        const value = event.currentTarget.value;
        return value === "" ? null : Number(value);
    };

    return (
        <div
            role="group"
            aria-labelledby={ariaLabelledBy}
            className={classNames("d-flex gap-1", { "flex-grow-1": isFlexGrow })}
        >
            {parts.map((part) => (
                <FormGroup
                    key={part.name}
                    controlId={controlIdPrefix + part.name}
                    className={classNames({ "flex-grow-1": isFlexGrow })}
                >
                    <InputGroup>
                        <Form.Control
                            type="number"
                            min={0}
                            value={part.value ?? ""}
                            placeholder={getPlaceholder(part.name)}
                            onChange={(e: ChangeEvent<HTMLInputElement>) => part.setValue(getInputValue(e))}
                            disabled={disabled}
                            aria-label={t(part.name)}
                        />
                        <InputGroup.Text>{t(part.name)}</InputGroup.Text>
                    </InputGroup>
                </FormGroup>
            ))}
        </div>
    );
}

function getInvalidPartMessageKey(parts: number[]): "nonNegativeNumberRequired" | "wholeNumberRequired" {
    const filledParts = parts.filter((part) => part != null);

    if (filledParts.some((part) => part < 0)) {
        return "nonNegativeNumberRequired";
    }
    if (filledParts.some((part) => !Number.isInteger(part))) {
        return "wholeNumberRequired";
    }
    return null;
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
