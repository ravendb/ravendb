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
}

export function RichPanelDetails(props: RichPanelDetailsProps) {
    const { children, ...rest } = props;
    return (
        <CardBody className="rich-panel-details" {...rest}>
            {children}
        </CardBody>
    );
}

export function RichPanelSelect(props: { children: ReactNode | ReactNode[] }) {
    return <FormGroup className="rich-panel-select form-check-secondary form-check-lg m-0">{props.children}</FormGroup>;
}

interface RichPanelDetailItemProps {
    id?: string;
    children: ReactNode | ReactNode[];
    className?: string;
}

export function RichPanelName(props: { children: ReactNode | ReactNode[] }) {
    return <h3 className="m-0 me-4">{props.children}</h3>;
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
