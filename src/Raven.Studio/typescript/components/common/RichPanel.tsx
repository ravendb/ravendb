import React, { ForwardedRef, forwardRef, HTMLAttributes, LegacyRef, ReactNode } from "react";
import "./RichPanel.scss";
import classNames from "classnames";
import { Badge, Card, CardHeader } from "reactstrap";

interface RichPanelProps {
    className?: string;
    children: ReactNode | ReactNode[];
    innerRef?: any;
    hover?: boolean;
    id?: string;
}

export function RichPanel(props: RichPanelProps) {
    const { children, className, innerRef, hover, id } = props;
    return (
        <Card
            className={classNames("rich-panel-item", className, { "rich-panel-hover": hover })}
            ref={innerRef}
            id={id}
        >
            {children}
        </Card>
    );
}

interface RichPanelStatusProps {
    color?: string;
    children: ReactNode | ReactNode[];
    className?: string;
    title?: string;
}

export function RichPanelStatus(props: RichPanelStatusProps) {
    const { children, className, color, ...rest } = props;
    return (
        <Badge className={classNames("rich-panel-status", className)} color={color} {...rest}>
            <span className="position-sticky">{children}</span>
        </Badge>
    );
}

interface RichPanelHeaderProps extends HTMLAttributes<HTMLDivElement> {
    className?: string;
    children: ReactNode | ReactNode[];
    id?: string;
}

export function RichPanelHeader(props: RichPanelHeaderProps) {
    const { children, className, ...rest } = props;
    return (
        <CardHeader className={classNames("rich-panel-header gap-2", className)} {...rest}>
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
    return <div className="rich-panel-select form-check-secondary form-check-lg">{props.children}</div>;
}

export function RichPanelInfo(props: { children: ReactNode | ReactNode[] }) {
    const { children, ...rest } = props;
    return (
        <div className="rich-panel-info" {...rest}>
            {children}
        </div>
    );
}

export function RichPanelActions(props: { children: ReactNode | ReactNode[] }) {
    const { children, ...rest } = props;
    return (
        <div className="rich-panel-actions" {...rest}>
            {children}
        </div>
    );
}

interface RichPanelNameProps {
    children: ReactNode | ReactNode[];
    className?: string;
    title?: string;
}

export function RichPanelName(props: RichPanelNameProps) {
    const { children, className, ...rest } = props;
    return (
        <h3 className={classNames("rich-panel-name", className)} {...rest}>
            {children}
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
    ref?: LegacyRef<HTMLDivElement>;
}

function RichPanelDetailItemInternal(props: RichPanelDetailItemProps, ref: ForwardedRef<HTMLDivElement>) {
    const { children, className, size, label, ...rest } = props;
    const panelClass = size ? "rich-panel-detail-item" + "-" + size : "rich-panel-detail-item";
    return (
        <div className={classNames(panelClass, className)} ref={ref} {...rest}>
            {label && <div className="small-label">{label}</div>}
            <div className="detail-item-content">{children}</div>
        </div>
    );
}

export const RichPanelDetailItem = forwardRef(RichPanelDetailItemInternal);
