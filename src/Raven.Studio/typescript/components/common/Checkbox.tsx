import React, { ChangeEvent, ReactNode, useEffect, useRef } from "react";
import { Input, InputProps, Label } from "reactstrap";
import useId from "hooks/useId";
import classNames from "classnames";

import "./Checkbox.scss";

export interface CheckboxProps extends Omit<InputProps, "className" | "children"> {
    selected: boolean;
    indeterminate?: boolean;
    toggleSelection: (x: ChangeEvent<HTMLInputElement>) => void;
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

    const defaultId = useId("checkbox");
    const inputEl = useRef<HTMLInputElement>();

    const inputId = id ?? defaultId;
    const checkboxClass = reverse ? `form-check-reverse` : "form-check";
    const colorClass = `form-check-${color ?? "secondary"}`;
    const sizeClass = size ? `form-check-${size}` : undefined;

    useEffect(() => {
        inputEl.current.indeterminate = indeterminate;
    }, [indeterminate]);

    return (
        <div className={classNames(checkboxClass, colorClass, sizeClass, className)}>
            <Input
                type="checkbox"
                innerRef={inputEl}
                id={inputId}
                checked={selected}
                onChange={toggleSelection}
                disabled={disabled}
                {...rest}
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
    const defaultId = useId("switch");

    const inputId = id ?? defaultId;
    const checkboxClass = reverse ? `form-check-reverse` : "form-check";
    const colorClass = `form-check-${color ?? "secondary"}`;
    const sizeClass = size ? `form-check-${size}` : undefined;

    return (
        <div className={classNames(colorClass, sizeClass, checkboxClass, "form-switch", className)}>
            <Input
                type="checkbox"
                id={inputId}
                checked={selected}
                onChange={toggleSelection}
                disabled={disabled}
                {...rest}
            />
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
    const defaultId = useId("radio");

    const inputId = id ?? defaultId;
    const checkboxClass = reverse ? `form-check-reverse` : "form-check";
    const colorClass = `form-check-${color ?? "secondary"}`;
    const sizeClass = size ? `form-check-${size}` : undefined;

    return (
        <div className={classNames(checkboxClass, colorClass, sizeClass, className)} {...rest}>
            <Input
                type="radio"
                id={inputId}
                checked={selected}
                onChange={toggleSelection}
                disabled={disabled}
                {...rest}
            />
            {children && (
                <Label check htmlFor={inputId}>
                    {children}
                </Label>
            )}
        </div>
    );
}
