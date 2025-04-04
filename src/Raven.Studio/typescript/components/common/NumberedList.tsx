import "./NumberedList.scss";
import { ReactElement, ReactNode } from "react";
import classNames from "classnames";

interface NumberedListProps {
    children: ReactElement<NumberedListItemProps>[];
    className?: string;
}

export function NumberedList(props: NumberedListProps) {
    return <ol className={classNames("numbered-list", props.className)}>{props.children}</ol>;
}

interface NumberedListItemProps {
    stepKey: number | string;
    children: ReactNode;
}

export function NumberedListItem(props: NumberedListItemProps) {
    return (
        <li className="numbered-list-item">
            <span className="dot-number">{props.stepKey}</span>
            {props.children}
        </li>
    );
}
