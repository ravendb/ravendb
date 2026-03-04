import React from "react";
import { Icon } from "components/common/Icon";

interface PopoverMessageProps {
    description: string | React.ReactNode;
    alert?: React.ReactNode;
    docsLink?: string;
}

export function SetupWizardInfoPopover({ description, docsLink, alert }: PopoverMessageProps) {
    return (
        <>
            <p className="mb-0">{description}</p>
            {alert}
            <hr className="my-2" />
            <span className="md-label">
                <Icon icon="link" />
                Read more in our{" "}
                <a href={docsLink} target="_blank" className="text-primary fw-bold">
                    documentation <Icon icon="newtab" />
                </a>
            </span>
        </>
    );
}
