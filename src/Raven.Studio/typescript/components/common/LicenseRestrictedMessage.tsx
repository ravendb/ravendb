import React, { ReactNode, PropsWithChildren } from "react";
import { Button } from "reactstrap";
import { Icon } from "./Icon";
import { useRavenLink } from "components/hooks/useRavenLink";

interface LicenseRestrictedMessageProps {
    featureName?: ReactNode | ReactNode[];
}

export function LicenseRestrictedMessage({ featureName }: PropsWithChildren<LicenseRestrictedMessageProps>) {
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    return (
        <>
            Current license doesn&apos;t include {featureName ?? "this feature"}.<br />
            <div className="text-center mt-1">
                <Button href={buyLink} target="_blank" color="primary" size="xs" className="rounded-pill">
                    Licensing options <Icon icon="newtab" margin="ms-1" />
                </Button>
            </div>
        </>
    );
}
