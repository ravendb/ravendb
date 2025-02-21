import "./NumberedList.scss";
import React, { ReactElement, ReactNode } from "react";
import classNames from "classnames";

interface NumberedListProps {
    children: ReactElement<NumberedListItemProps>[];
    className?: string;
}

export function NumberedList(props: NumberedListProps) {
    const { children, className } = props;

    return (
        <ol className={classNames("numbered-list", className)}>
            {React.Children.map(children, (child, index) => {
                if (!React.isValidElement(child)) {
                    return null;
                }

                const customNumber = child.props.stepKey;

                return React.cloneElement(child, {
                    stepKey: customNumber == null ? index + 1 : customNumber,
                });
            })}
        </ol>
    );
}

interface NumberedListItemProps {
    stepKey?: number | string;
    children: ReactNode;
}

export function NumberedListItem(props: NumberedListItemProps) {
    const { stepKey = 0, children } = props;

    return (
        <li className="numbered-list-item">
            <span className="dot-number">{stepKey}</span>
            {children}
        </li>
    );
}
