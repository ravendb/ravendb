import React, { ReactNode } from "react";
import "./RichPanel.scss";
import classNames from "classnames";

interface RichPanelProps {
    className?: string;
    children: ReactNode | ReactNode[];
    ref?: any;
}

//TODO: forward ref

export function RichPanel(props: RichPanelProps) {
    const { children, className, ref } = props;
    return (
        <div className={classNames("rich-panel-item", className)} ref={ref}>
            {children}
        </div>
    );
}

interface RichPanelHeaderProps {
    children: ReactNode | ReactNode[];
    id?: string;
}

export function RichPanelHeader(props: RichPanelHeaderProps) {
    const { children, ...rest } = props;
    return (
        <div className="rich-panel-header" {...rest}>
            {children}
        </div>
    );
}

interface RichPanelDetailsProps {
    children: ReactNode | ReactNode[];
}

export function RichPanelDetails(props: RichPanelDetailsProps) {
    const { children, ...rest } = props;
    return (
        <div className="rich-panel-details" {...rest}>
            {children}
        </div>
    );
}

export function RichPanelSelect(props: { children: ReactNode | ReactNode[] }) {
    return <div className="rich-panel-select">{props.children}</div>;
}

interface RichPanelDetailItemProps {
    id?: string;
    children: ReactNode | ReactNode[];
    className?: string;
}

export function RichPanelDetailItem(props: RichPanelDetailItemProps) {
    const { children, className, ...rest } = props;
    return (
        <div className={classNames("rich-panel-detail-item", className)} {...rest}>
            {children}
        </div>
    );
}
