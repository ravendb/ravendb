import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import { Icon } from "components/common/Icon";
import React from "react";

export default function EncryptionUnavailableMessage() {
    return (
        <LicenseRestrictedMessage
            featureName={
                <strong className="text-primary">
                    <Icon icon="storage" addon="encryption" margin="m-0" /> Storage encryption
                </strong>
            }
        />
    );
}
