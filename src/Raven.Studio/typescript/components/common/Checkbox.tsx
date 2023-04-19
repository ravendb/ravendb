import React, { HTMLAttributes, ReactNode, useEffect, useRef } from "react";
import { Input, Label } from "reactstrap";
import useId from "hooks/useId";
import classNames from "classnames";

import "./Checkbox.scss";

interface CheckboxProps extends Omit<HTMLAttributes<HTMLDivElement>, "className" | "children"> {
    selected: boolean;
    indeterminate?: boolean;
    toggleSelection: () => void;
    children?: ReactNode | ReactNode[];
    color?: string;
    size?: string;
    reverse?: boolean;
    disabled?: boolean;
    className?: string;
    id?: string;
}

export function Checkbox(props: CheckboxProps) {
    const {
        selected,
        indeterminate,
        toggleSelection,
        children,
        color,
        size,
        reverse,
        className,
        disabled,
        id,
        ...rest
    } = props;

    // eslint-disable-next-line react-hooks/rules-of-hooks
    const inputId = id ?? useId("checkbox");
    const checkboxClass = reverse ? `form-check-reverse` : "form-check";
    const colorClass = `form-check-${color ?? "secondary"}`;
    const sizeClass = size ? `form-check-${size}` : undefined;

    const inputEl = useRef<HTMLInputElement>();

    useEffect(() => {
        inputEl.current.indeterminate = indeterminate;
    }, [indeterminate]);

    return (
        <div className={classNames(checkboxClass, colorClass, sizeClass, className)} {...rest}>
            <Input
                type="checkbox"
                innerRef={inputEl}
                id={inputId}
                checked={selected}
                onChange={toggleSelection}
                disabled={disabled}
            />
            {children && (
                <Label check htmlFor={inputId}>
                    {children}
                </Label>
            )}
        </div>
    );
}

export function Switch(props: CheckboxProps) {
    const { selected, toggleSelection, children, color, size, reverse, className, disabled, id, ...rest } = props;
    // eslint-disable-next-line react-hooks/rules-of-hooks
    const inputId = id ?? useId("checkbox");

    const checkboxClass = reverse ? `form-check-reverse` : "form-check";
    const colorClass = `form-check-${color ?? "secondary"}`;
    const sizeClass = size ? `form-check-${size}` : undefined;

    return (
        <div className={classNames(colorClass, sizeClass, checkboxClass, "form-switch", className)} {...rest}>
            <Input type="checkbox" id={inputId} checked={selected} onChange={toggleSelection} disabled={disabled} />
            {children && (
                <Label check htmlFor={inputId}>
                    {children}
                </Label>
            )}
        </div>
    );
}

export function Radio(props: CheckboxProps) {
    const { selected, toggleSelection, children, color, size, reverse, className, disabled, id, ...rest } = props;

    // eslint-disable-next-line react-hooks/rules-of-hooks
    const inputId = id ?? useId("checkbox");

    const checkboxClass = reverse ? `form-check-reverse` : "form-check";
    const colorClass = `form-check-${color ?? "secondary"}`;
    const sizeClass = size ? `form-check-${size}` : undefined;

    return (
        <div className={classNames(checkboxClass, colorClass, sizeClass, className)} {...rest}>
            <Input type="radio" id={inputId} checked={selected} onChange={toggleSelection} disabled={disabled} />
            {children && (
                <Label check htmlFor={inputId}>
                    {children}
                </Label>
            )}
        </div>
    );
}
