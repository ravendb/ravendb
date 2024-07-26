import { ConditionalPopover } from "components/common/ConditionalPopover";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import React from "react";
import { Button } from "reactstrap";
import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";

interface IntegrationsAddNewButtonProps {
    isLicenseUpgradeRequired: boolean;
    addNewUser: () => void;
}

export default function IntegrationsAddNewButton(props: IntegrationsAddNewButtonProps) {
    const { isLicenseUpgradeRequired, addNewUser } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    return (
        <ConditionalPopover
            conditions={[
                {
                    isActive: isLicenseUpgradeRequired,
                    message: (
                        <LicenseRestrictedMessage>
                            Your license doesn&apos;t allow adding new credentials.
                        </LicenseRestrictedMessage>
                    ),
                },
                { isActive: !hasDatabaseAdminAccess, message: "You don't have permissions to add new credentials." },
            ]}
        >
            <Button
                color="info"
                size="sm"
                className="rounded-pill"
                title="Add new credentials"
                onClick={addNewUser}
                disabled={isLicenseUpgradeRequired || !hasDatabaseAdminAccess}
            >
                <Icon icon="plus" />
                Add new
            </Button>
        </ConditionalPopover>
    );
}
