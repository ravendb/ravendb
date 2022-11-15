import React, { ReactNode } from "react";
import { FormGroup, Input, Label } from "reactstrap";

interface CheckboxProps {
    selected: boolean;
    toggleSelection: () => void;
    children?: ReactNode | ReactNode[];
    color?: string;
}

export function Checkbox(props: CheckboxProps) {
    const { selected, toggleSelection, children, color, ...rest } = props;
    const colorClass = `form-check-${color ?? "secondary"}`;
    return (
        <FormGroup check>
            <Input type="checkbox" checked={selected} onChange={toggleSelection} className={colorClass} {...rest} />
            <Label check>{children}</Label>
        </FormGroup>
    );
}
