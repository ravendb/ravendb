import React, { ReactNode } from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { ClickPopover } from "components/common/ClickPopover";

interface WarningDetailsProps {
    details: string;
    title: string;
    children: ReactNode;
}

export function WarningDetails({ details, title, children }: WarningDetailsProps) {
    return (
        <ClickPopover content={details}>
            <Button
                variant="link"
                size="sm"
                className="d-flex align-items-center p-0 text-warning text-start"
                title={title}
            >
                <Icon icon="warning" margin="me-1" />
                {children}
            </Button>
        </ClickPopover>
    );
}
