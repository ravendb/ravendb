import { Icon } from "components/common/Icon";
import { useAppUrls } from "components/hooks/useAppUrls";
import React from "react";

export default function AuthenticationOffMessage() {
    const { appUrl } = useAppUrls();

    return (
        <>
            <p className="lead text-warning">
                <Icon icon="unsecure" margin="m-0" /> Authentication is off
            </p>
            <p>
                <strong>Encription at Rest</strong> is only possible when authentication is enabled and a server
                certificate has been defined.
            </p>
            <p>
                For more information go to the <a href={appUrl.forCertificates()}>certificates page</a>
            </p>
        </>
    );
}
