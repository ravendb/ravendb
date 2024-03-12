import { useAsync } from "react-async-hook";
import { useCallback } from "react";
import { useServices } from "hooks/useServices";
import licenseModel from "models/auth/licenseModel";

export function useAboutPage() {
    const { licenseService } = useServices();
    const fetchLatestVersion = useCallback(async (refresh: boolean) => licenseService.getLatestVersion(refresh), []);
    const asyncFetchLatestVersion = useAsync(fetchLatestVersion, [false]);
    const asyncGetConfigurationSettings = useAsync(licenseService.getConfigurationSettings, []);

    useAsync(async () => licenseService.getLicenseStatus(), [], {
        onSuccess: (result) => licenseModel.licenseStatus(result),
    });

    const checkLicenseServerConnectivity = useCallback(async () => licenseService.checkLicenseServerConnectivity(), []);
    const asyncCheckLicenseServerConnectivity = useAsync(checkLicenseServerConnectivity, []);

    return { asyncFetchLatestVersion, asyncCheckLicenseServerConnectivity, asyncGetConfigurationSettings };
}
