import classNames from "classnames";
import React, { ChangeEvent, useEffect, useEffectEvent, useState } from "react";
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
    id?: string;
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

const durationParts: DurationPart[] = ["days", "hours", "minutes", "seconds"];

const emptyDuration: Duration = {
    days: null,
    hours: null,
    minutes: null,
    seconds: null,
};

export default function DurationPicker(props: DurationPickerProps) {
    const {
        totalSeconds,
        onChange,
        id,
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

    const [duration, setDuration] = useState(() => toDuration(totalSeconds, showDays));
    const [syncedTotalSeconds, setSyncedTotalSeconds] = useState(totalSeconds ?? null);

    const consumerTotalSeconds = totalSeconds ?? null;
    if (!Object.is(consumerTotalSeconds, syncedTotalSeconds)) {
        setSyncedTotalSeconds(consumerTotalSeconds);
        setDuration(toDuration(consumerTotalSeconds, showDays));
    }

    const clearInvalidTotalSeconds = useEffectEvent(() => {
        if (Number.isNaN(totalSeconds)) {
            onChange(null);
        }
    });

    useEffect(() => () => clearInvalidTotalSeconds(), []);

    const isPartVisible: Record<DurationPart, boolean> = {
        days: showDays,
        hours: true,
        minutes: true,
        seconds: showSeconds,
    };
    const parts = durationParts.filter((part) => isPartVisible[part]);

    const getPlaceholder = (part: DurationPart): string => {
        if (defaultTotalSeconds == null) {
            return placeholder?.[part];
        }
        if (!isEmpty(duration, parts)) {
            return ZERO_DURATION_PLACEHOLDER[part];
        }
        return commonT("defaultPlaceholder", { value: toDuration(defaultTotalSeconds, showDays)[part] });
    };

    const handlePartChange = (part: DurationPart, inputValue: string) => {
        const newDuration = { ...duration, [part]: inputValue === "" ? null : Number(inputValue) };
        const newTotalSeconds = getTotalSeconds(newDuration, parts);
        const invalidPartMessageKey = getInvalidPartMessageKey(newDuration, parts);

        setDuration(newDuration);
        setSyncedTotalSeconds(newTotalSeconds);
        onInvalidPartMessageChange?.(invalidPartMessageKey ? t(invalidPartMessageKey) : null);
        onChange(newTotalSeconds);
    };

    return (
        <div
            role="group"
            aria-labelledby={ariaLabelledBy}
            className={classNames("d-flex gap-1", { "flex-grow-1": isFlexGrow })}
        >
            {parts.map((part, index) => (
                <FormGroup
                    key={part}
                    controlId={index === 0 && id ? id : controlIdPrefix + part}
                    className={classNames({ "flex-grow-1": isFlexGrow })}
                >
                    <InputGroup>
                        <Form.Control
                            type="number"
                            min={0}
                            value={duration[part] ?? ""}
                            placeholder={getPlaceholder(part)}
                            onChange={(e: ChangeEvent<HTMLInputElement>) =>
                                handlePartChange(part, e.currentTarget.value)
                            }
                            disabled={disabled}
                            aria-label={t(part)}
                        />
                        <InputGroup.Text>{t(part)}</InputGroup.Text>
                    </InputGroup>
                </FormGroup>
            ))}
        </div>
    );
}

function isEmpty(duration: Duration, parts: DurationPart[]): boolean {
    return parts.every((part) => duration[part] == null);
}

function getInvalidPartMessageKey(
    duration: Duration,
    parts: DurationPart[]
): "nonNegativeNumberRequired" | "wholeNumberRequired" {
    const filledValues = parts.map((part) => duration[part]).filter((value) => value != null);

    if (filledValues.some((value) => value < 0)) {
        return "nonNegativeNumberRequired";
    }
    if (filledValues.some((value) => !Number.isInteger(value))) {
        return "wholeNumberRequired";
    }
    return null;
}

function getTotalSeconds(duration: Duration, parts: DurationPart[]): number {
    if (isEmpty(duration, parts)) {
        return null;
    }
    if (getInvalidPartMessageKey(duration, parts)) {
        return NaN;
    }

    const { days, hours, minutes, seconds } = duration;
    return seconds + minutes * 60 + hours * 60 * 60 + days * 24 * 60 * 60;
}

function toDuration(totalSeconds: number, showDays: boolean): Duration {
    if (totalSeconds == null || Number.isNaN(totalSeconds)) {
        return emptyDuration;
    }

    const wholeSeconds = Math.round(totalSeconds);
    const totalHours = Math.floor(wholeSeconds / 3600);

    return {
        days: showDays ? Math.floor(totalHours / 24) : 0,
        hours: showDays ? totalHours % 24 : totalHours,
        minutes: Math.floor(wholeSeconds / 60) % 60,
        seconds: wholeSeconds % 60,
    };
}
