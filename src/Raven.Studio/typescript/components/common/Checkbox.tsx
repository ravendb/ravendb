import React, { ReactNode } from "react";
import { FormGroup, Input, Label, Row } from "reactstrap";

interface CheckboxProps {
    selected: boolean;
    toggleSelection: () => void;
    children?: ReactNode | ReactNode[];
    color?: string;
}

export function Checkbox(props: CheckboxProps) {
    const { selected, toggleSelection, children, color } = props;
    const colorClass = `form-check-${color ?? "secondary"}`;
    return (
        <div className="form-check">
            <Input type="checkbox" checked={selected} onChange={toggleSelection} className={colorClass} />
            <Label check>{children}</Label>
        </div>
    );
}
