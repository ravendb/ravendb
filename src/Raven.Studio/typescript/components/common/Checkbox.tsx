import React, { ReactNode } from "react";
import { Input, Label } from "reactstrap";
import useId from "hooks/useId";
import classNames from "classnames";

interface CheckboxProps {
    selected: boolean;
    toggleSelection: () => void;
    children?: ReactNode | ReactNode[];
    color?: string;
    className?: string;
}

export function Checkbox(props: CheckboxProps) {
    const { selected, toggleSelection, children, color, className } = props;
    const inputId = useId("checkbox");

    const colorClass = `form-check-${color ?? "secondary"}`;
    return (
        <div className={classNames(colorClass, className)}>
            <Input type="checkbox" checked={selected} onChange={toggleSelection} />
            {children && (
                <Label check htmlFor={inputId} className="ms-2">
                    {children}
                </Label>
            )}
        </div>
    );
}
