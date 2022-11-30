import React, { ReactNode } from "react";
import "./RichPanel.scss";
import classNames from "classnames";
import { Card, CardBody, CardHeader, FormGroup } from "reactstrap";

interface RichPanelProps {
    className?: string;
    children: ReactNode | ReactNode[];
    innerRef?: any;
}

export function RichPanel(props: RichPanelProps) {
    const { children, className, innerRef } = props;
    return (
        <Card className={classNames("rich-panel-item", className)} ref={innerRef}>
            {children}
        </Card>
    );
}

interface RichPanelHeaderProps {
    className?: string;
    children: ReactNode | ReactNode[];
    id?: string;
}

export function RichPanelHeader(props: RichPanelHeaderProps) {
    const { children, className, ...rest } = props;
    return (
        <CardHeader className={classNames("rich-panel-header", className)} {...rest}>
            {children}
        </CardHeader>
    );
}

interface RichPanelDetailsProps {
    children: ReactNode | ReactNode[];
    className?: string;
}

export function RichPanelDetails(props: RichPanelDetailsProps) {
    const { children, className, ...rest } = props;
    return (
        <div className={classNames("rich-panel-details", className)} {...rest}>
            {children}
        </div>
    );
}

export function RichPanelSelect(props: { children: ReactNode | ReactNode[] }) {
    return <div className="rich-panel-select form-check-secondary form-check-lg m-0">{props.children}</div>;
}

interface RichPanelDetailItemProps {
    id?: string;
    children: ReactNode | ReactNode[];
    className?: string;
}

interface RichPanelNameProps {
    children: ReactNode | ReactNode[];
    title?: string;
}

export function RichPanelName(props: RichPanelNameProps) {
    const { children, ...rest } = props;
    return (
        <h3 className="m-0 me-4 flex-grow-1" {...rest}>
            {props.children}
        </h3>
    );
}

interface RichPanelDetailItemProps {
    id?: string;
    size?: string;
    children: ReactNode | ReactNode[];
    className?: string;
    label?: ReactNode | ReactNode[];
    title?: string;
}

export function RichPanelDetailItem(props: RichPanelDetailItemProps) {
    const { children, className, size, label, ...rest } = props;
    const panelClass = size ? "rich-panel-detail-item" + "-" + size : "rich-panel-detail-item";
    return (
        <div className={classNames(panelClass, className)} {...rest}>
            {label && <div className="detail-item-label">{label}</div>}
            <div className="detail-item-content">{children}</div>
        </div>
    );
}
