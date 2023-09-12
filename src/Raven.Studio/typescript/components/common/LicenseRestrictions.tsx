import React, { ReactNode } from "react";
import { Button, PopoverBody, UncontrolledPopover } from "reactstrap";
import useId from "hooks/useId";
import { Icon } from "./Icon";
import classNames from "classnames";
import { useRavenLink } from "components/hooks/useRavenLink";

interface LicenseRestrictionsProps {
    children: ReactNode | ReactNode[];
    isAvailable?: boolean;
    featureName?: string | ReactNode | ReactNode[];
    message?: string | ReactNode | ReactNode[];
    className?: string;
}

export function LicenseRestrictions(props: LicenseRestrictionsProps): JSX.Element {
    const { children, isAvailable, featureName, message, className } = props;

    const containerId = useId("Info");
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    if (!isAvailable) {
        return (
            <>
                <div id={containerId} className={classNames("item-disabled", className)}>
                    {children}
                </div>
                <UncontrolledPopover
                    target={containerId}
                    trigger="hover"
                    placement="top"
                    container="PopoverContainer" //TODO add containers in main layout or remove this part
                >
                    <PopoverBody>
                        {message ? (
                            <>{message}</>
                        ) : (
                            <>
                                Current license doesn&apos;t include {featureName ?? "this feature"}.<br />
                                <div className="text-center mt-1">
                                    <Button
                                        href={buyLink}
                                        target="_blank"
                                        color="primary"
                                        size="xs"
                                        className="rounded-pill"
                                    >
                                        Licensing options <Icon icon="newtab" margin="ms-1" />
                                    </Button>
                                </div>
                            </>
                        )}
                    </PopoverBody>
                </UncontrolledPopover>
            </>
        );
    }

    return <>{children}</>;
}
