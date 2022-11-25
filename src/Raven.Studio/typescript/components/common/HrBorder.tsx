import React, { ReactNode } from "react";
import { FormGroup, Input, Label, Row } from "reactstrap";

interface HrBorderProps {
    children?: ReactNode | ReactNode[];
    right?: ReactNode | ReactNode[];
}

export function HrBorder(props: HrBorderProps) {
    const { right, children } = props;

    return (
        <div>
            {children && <h5 className="ms-2">{children}</h5>}
            <hr />
            {right && { right }}
        </div>
    );
}
