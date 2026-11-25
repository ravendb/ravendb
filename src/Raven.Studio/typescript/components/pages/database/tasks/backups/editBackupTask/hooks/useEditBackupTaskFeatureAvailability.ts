import IconName from "typings/server/icons";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface BackupLicenseFeature {
    name: string;
    icon: IconName;
    isInProfessional: boolean;
    isInCurrentLicense: boolean;
}

export function useEditBackupTaskFeatureAvailability(isManual: boolean) {
    const t = useStudioTranslation("editBackupTask");

    const hasPeriodicBackup = useAppSelector(licenseSelectors.statusValue("HasPeriodicBackup"));
    const hasCloudBackups = useAppSelector(licenseSelectors.statusValue("HasCloudBackups"));
    const hasEncryptedBackups = useAppSelector(licenseSelectors.statusValue("HasEncryptedBackups"));
    const hasSnapshotBackups = useAppSelector(licenseSelectors.statusValue("HasSnapshotBackups"));

    const periodicFeature: BackupLicenseFeature = {
        name: t("infoHub.features.periodicBackups"),
        icon: "periodic-backup",
        isInProfessional: true,
        isInCurrentLicense: hasPeriodicBackup,
    };

    const features: BackupLicenseFeature[] = [
        ...(isManual ? [] : [periodicFeature]),
        {
            name: t("infoHub.features.cloudDestinations"),
            icon: "cloud",
            isInProfessional: true,
            isInCurrentLicense: hasCloudBackups,
        },
        {
            name: t("infoHub.features.encryptedBackups"),
            icon: "encryption",
            isInProfessional: true,
            isInCurrentLicense: hasEncryptedBackups,
        },
        {
            name: t("infoHub.features.snapshotBackups"),
            icon: "snapshot-backup",
            isInProfessional: false,
            isInCurrentLicense: hasSnapshotBackups,
        },
    ];

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability: features.map((feature) => ({
            featureName: feature.name,
            featureIcon: feature.icon,
            community: { value: false },
            professional: { value: feature.isInProfessional },
            enterprise: { value: true },
            quill: { value: true },
        })),
        overwrites: features.map((feature) => ({ featureName: feature.name, value: feature.isInCurrentLicense })),
    });

    return {
        featureAvailability,
        isUnlimited: features.every((feature) => feature.isInCurrentLicense),
        isOpenedByDefault: features.every((feature) => !feature.isInCurrentLicense),
    };
}
