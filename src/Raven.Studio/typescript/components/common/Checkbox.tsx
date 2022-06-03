import React, { ReactNode } from "react";

interface CheckboxProps {
    selected: boolean;
    toggleSelection: () => void;
    children?: ReactNode | ReactNode[];
}

export function Checkbox(props: CheckboxProps) {
    const { selected, toggleSelection, children } = props;
    return (
        <div className="checkbox">
            <input type="checkbox" className="styled" checked={selected} onChange={toggleSelection} />
            <label>{children}</label>
        </div>
    );
}
