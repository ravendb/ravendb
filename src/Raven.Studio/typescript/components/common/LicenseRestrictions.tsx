import React, { ReactNode } from "react";
import { Button, PopoverBody, UncontrolledPopover } from "reactstrap";
import useId from "hooks/useId";
import { Icon } from "./Icon";

interface LicenseRestrictionsProps {
    children: ReactNode | ReactNode[];
    isAvailable?: boolean;
    featureName?: string | ReactNode | ReactNode[];
    message?: string | ReactNode | ReactNode[];
}

export function LicenseRestrictions(props: LicenseRestrictionsProps): JSX.Element {
    const { children, isAvailable, featureName, message } = props;
    const containerId = useId("Info");
    if (!isAvailable) {
        return (
            <>
                <div id={containerId} className="item-disabled">
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
                                Current license doesn't include {featureName ? featureName : "this feature"}.<br />
                                <div className="text-center mt-1">
                                    <Button
                                        href="https://ravendb.net/buy"
                                        target="_blank"
                                        color="primary"
                                        size="xs"
                                        className="rounded-pill"
                                    >
                                        Licensing options <Icon icon="newtab" />
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
