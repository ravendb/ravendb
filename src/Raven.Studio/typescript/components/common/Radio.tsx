import React, { ReactNode } from "react";
import { Input, Label } from "reactstrap";
import useId from "hooks/useId";

interface RadioProps {
    selected?: boolean;
    toggleSelection: () => void;
    children?: ReactNode | ReactNode[];
    color?: string;
}

export function Radio(props: RadioProps) {
    const { selected, toggleSelection, children, color } = props;
    const inputId = useId("radio");

    const colorClass = `form-check-${color ?? "secondary"}`;
    return (
        <div className={colorClass}>
            <Input type="radio" checked={selected} onChange={toggleSelection} />
            {children && (
                <Label check htmlFor={inputId} className="ms-2">
                    {children}
                </Label>
            )}
        </div>
    );
}
