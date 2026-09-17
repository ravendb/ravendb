import { useMemo } from "react";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { DatabaseSettingKey } from "./importFromFileValidation";
import { databaseSettingLabels } from "./importFromFileLabels";

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
    const licenseStatus = useAppSelector(licenseSelectors.status);

    return useMemo(
        () =>
            minPeriodSettings.flatMap(({ key, licenseKey }) => {
                const minPeriodInHours = licenseStatus?.[licenseKey];

                if (typeof minPeriodInHours !== "number") {
                    return [];
                }

                return [{ key, label: databaseSettingLabels[key], minPeriodInHours }];
            }),
        [licenseStatus]
    );
}
