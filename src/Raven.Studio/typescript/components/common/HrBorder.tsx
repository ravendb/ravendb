import React, { ReactNode } from "react";
import { FormGroup, Input, Label, Row } from "reactstrap";

interface HrBorderProps {
    selected: boolean;
    toggleSelection: () => void;
    children?: ReactNode | ReactNode[];
    color?: string;
}

export function HrBorder(props: HrBorderProps) {
    const { selected, toggleSelection, children, color } = props;
    const colorClass = `form-check-${color ?? "secondary"}`;
    return (
        <div className={colorClass}>
            <Input type="checkbox" checked={selected} onChange={toggleSelection} />
            {children && (
                <Label check className="ms-2">
                    {children}
                </Label>
            )}
        </div>
    );
}
