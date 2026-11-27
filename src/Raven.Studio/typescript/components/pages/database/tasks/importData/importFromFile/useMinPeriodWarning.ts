import { useMemo } from "react";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { DatabaseSettingKey } from "components/pages/database/tasks/importData/shared/importOptionsValidation";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

type MinPeriodLicenseKey = "MinPeriodForRefreshInHours" | "MinPeriodForExpirationInHours";

const minPeriodSettings = [
    { key: "refresh", licenseKey: "MinPeriodForRefreshInHours" },
    { key: "expiration", licenseKey: "MinPeriodForExpirationInHours" },
] as const satisfies readonly { key: DatabaseSettingKey; licenseKey: MinPeriodLicenseKey }[];

export interface MinPeriodWarning {
    key: DatabaseSettingKey;
    label: string;
    minPeriodInHours: number;
}

export function useMinPeriodWarning(): MinPeriodWarning[] {
    const t = useStudioTranslation("importData");
    const licenseStatus = useAppSelector(licenseSelectors.status);

    return useMemo(
        () =>
            minPeriodSettings.flatMap(({ key, licenseKey }) => {
                const minPeriodInHours = licenseStatus?.[licenseKey];

                if (typeof minPeriodInHours !== "number") {
                    return [];
                }

                return [{ key, label: t(`databaseSettings.${key}`), minPeriodInHours }];
            }),
        [licenseStatus, t]
    );
}
