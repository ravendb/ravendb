import React, { ChangeEvent, ReactNode, useEffect, useRef } from "react";
import Form from "react-bootstrap/Form";
import useUniqueId from "components/hooks/useUniqueId";
import classNames from "classnames";
import "./Checkbox.scss";
import { RavenFormControlProps } from "react-bootstrap/FormControl";
import { InputType } from "../../../typings/_studio/react-bootstrap";

export interface CheckboxProps extends Omit<RavenFormControlProps, "className" | "children"> {
    selected: boolean;
    indeterminate?: boolean;
    toggleSelection: (x: ChangeEvent<HTMLInputElement>) => void;
    children?: ReactNode | ReactNode[];
    color?: string;
    type?: Extract<InputType, "checkbox" | "switch" | "radio">;
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

    const defaultId = useUniqueId("checkbox");
    const inputEl = useRef<HTMLInputElement>(null);

    const inputId = id ?? defaultId;
    const checkboxClass = reverse ? `form-check-reverse` : "form-check";
    const colorClass = `form-check-${color ?? "secondary"}`;
    const sizeClass = size ? `form-check-${size}` : undefined;

    useEffect(() => {
        inputEl.current.indeterminate = indeterminate;
    }, [indeterminate]);

    return (
        <div className={classNames(checkboxClass, colorClass, sizeClass, className)}>
            <Form.Check
                type="checkbox"
                ref={inputEl}
                id={inputId}
                checked={selected}
                label={children}
                onChange={toggleSelection}
                disabled={disabled}
                {...rest}
            />
        </div>
    );
}

export function Switch(props: CheckboxProps) {
    const { selected, toggleSelection, children, color, size, reverse, className, disabled, id, ...rest } = props;
    const defaultId = useUniqueId("switch");

    const inputId = id ?? defaultId;
    const checkboxClass = reverse ? `form-check-reverse` : "form-check";
    const colorClass = `form-check-${color ?? "secondary"}`;
    const sizeClass = size ? `form-check-${size}` : undefined;

    return (
        <div className={classNames(colorClass, sizeClass, checkboxClass, "form-switch", className)}>
            <Form.Check
                type="switch"
                id={inputId}
                checked={selected}
                onChange={toggleSelection}
                label={children}
                disabled={disabled}
                {...rest}
            />
        </div>
    );
}

export function Radio(props: CheckboxProps) {
    const { selected, toggleSelection, children, color, size, reverse, className, disabled, id, ...rest } = props;
    const defaultId = useUniqueId("radio");

    const inputId = id ?? defaultId;
    const checkboxClass = reverse ? `form-check-reverse` : "form-check";
    const colorClass = `form-check-${color ?? "secondary"}`;
    const sizeClass = size ? `form-check-${size}` : undefined;

    return (
        <div className={classNames(checkboxClass, colorClass, sizeClass, className)}>
            <Form.Check
                type="radio"
                id={inputId}
                checked={selected}
                onChange={toggleSelection}
                label={children}
                disabled={disabled}
                {...rest}
            />
        </div>
    );
}
