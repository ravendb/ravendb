import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { useState } from "react";
import { useAsync } from "react-async-hook";

export interface IntegrationsUser {
    id: string;
    username: string;
}

export function useIntegrations() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasPostgreSqlIntegration = useAppSelector(licenseSelectors.statusValue("HasPostgreSqlIntegration"));
    const hasPowerBi = useAppSelector(licenseSelectors.statusValue("HasPowerBI"));

    const isSharded = db.isSharded || DatabaseUtils.isSharded(db.name);
    const isLicenseUpgradeRequired = !hasPostgreSqlIntegration && !hasPowerBi;

    const { databasesService } = useServices();

    const asyncGetIsPostgreSqlSupportEnabled = useAsync(async () => {
        if (isLicenseUpgradeRequired || isSharded) {
            return false;
        }

        const result = await databasesService.getIntegrationsPostgreSqlSupport(db.name);
        return result.Active;
    }, [db.name, isLicenseUpgradeRequired]);

    const asyncGetPostgreSqlUsers = useAsync(
        async () => {
            if (isLicenseUpgradeRequired || isSharded) {
                return [];
            }

            const result = await databasesService.getIntegrationsPostgreSqlCredentials(db.name);
            return (
                result?.Users?.map((x) => ({
                    id: createId(),
                    username: x.Username,
                })) ?? []
            );
        },
        [db.name, isLicenseUpgradeRequired],
        {
            onSuccess(result) {
                setUsers(result);
            },
        }
    );

    const [users, setUsers] = useState<IntegrationsUser[]>([]);

    const addNewUser = () => {
        setUsers((prev) => [{ id: createId(), username: "" }, ...prev]);
    };

    const removeUser = (index: number) => {
        setUsers((prev) => prev.filter((_, i) => i !== index));
    };

    return {
        isSharded,
        isLicenseUpgradeRequired,
        isPostgreSqlSupportEnabled: asyncGetIsPostgreSqlSupportEnabled.result,
        asyncGetPostgreSqlUsers,
        users,
        addNewUser,
        removeUser,
    };
}

function createId() {
    return _.uniqueId("integrations-user-");
}
