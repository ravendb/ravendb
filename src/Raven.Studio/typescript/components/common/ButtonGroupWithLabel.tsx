import React from "react";
import { ButtonGroup, ButtonGroupProps } from "reactstrap";
import classNames from "classnames";

interface ButtonGroupWithLabelProps extends ButtonGroupProps {
    label?: string;
}

export function ButtonGroupWithLabel(props: ButtonGroupWithLabelProps) {
    const { children, label, className, ...rest } = props;

    return (
        <ButtonGroup className={classNames("flex-vertical btn-group-label", className)} data-label={label} {...rest}>
            <div className="d-flex gap-1 flex-wrap">{children}</div>
        </ButtonGroup>
    );
}
