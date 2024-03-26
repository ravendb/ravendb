import { ConditionalPopover } from "components/common/ConditionalPopover";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import React from "react";
import { Button } from "reactstrap";

interface IntegrationsAddNewButtonProps {
    isLicenseUpgradeRequired: boolean;
    addNewUser: () => void;
}

export default function IntegrationsAddNewButton(props: IntegrationsAddNewButtonProps) {
    const { isLicenseUpgradeRequired, addNewUser } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.hasDatabaseAdminAccess());

    return (
        <ConditionalPopover
            conditions={[
                {
                    isActive: isLicenseUpgradeRequired,
                    message: "You need to upgrade your license to add new credentials.",
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
