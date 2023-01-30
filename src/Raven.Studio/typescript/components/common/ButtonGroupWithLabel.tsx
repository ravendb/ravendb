import React from "react";
import { ButtonGroup, ButtonGroupProps } from "reactstrap";

interface ButtonGroupWithLabelProps extends ButtonGroupProps {
    label?: string;
}

export function ButtonGroupWithLabel(props: ButtonGroupWithLabelProps) {
    const { children, label, ...rest } = props;

    return (
        <ButtonGroup className="flex-vertical btn-group-label" data-label={label} {...rest}>
            <div className="d-flex gap-1 flex-wrap">{children}</div>
        </ButtonGroup>
    );
}
