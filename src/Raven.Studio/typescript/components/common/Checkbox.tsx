import React, { ReactNode } from "react";
import { FormGroup, Input, Label, Row } from "reactstrap";
import useId from "hooks/useId";

interface CheckboxProps {
    selected: boolean;
    toggleSelection: () => void;
    children?: ReactNode | ReactNode[];
    color?: string;
}

export function Checkbox(props: CheckboxProps) {
    const { selected, toggleSelection, children, color } = props;
    const inputId = useId("checkbox");

    const colorClass = `form-check-${color ?? "secondary"}`;
    return (
        <div className="form-check">
            <Input type="checkbox" checked={selected} onChange={toggleSelection} className={colorClass} id={inputId} />
            <Label check htmlFor={inputId}>
                {children}
            </Label>
        </div>
    );
}
