import React, { PropsWithChildren } from "react";
import { Icon } from "./Icon";
import { useRavenLink } from "components/hooks/useRavenLink";

export function LicenseRestrictedMessage({ children }: PropsWithChildren) {
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    return (
        <>
            {children}
            <div className="mt-2">
                <a href={buyLink} target="_blank" className="btn btn-primary btn-sm rounded-pill">
                    Licensing options <Icon icon="newtab" margin="ms-1" />
                </a>
            </div>
        </>
    );
}
