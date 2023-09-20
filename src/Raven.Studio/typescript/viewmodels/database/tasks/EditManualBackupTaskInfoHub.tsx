import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {FeatureAvailabilityData} from "components/common/FeatureAvailabilitySummary";

export function EditManualBackupTaskInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove);
    const backupsDocsLink = useRavenLink({ hash: "GMBYOH" });
    const featureAvailabilityData: FeatureAvailabilityData[] = [
        {
            featureName: "Encryption",
            featureIcon: "encryption",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true }
        },{
            featureName: "Remote destinations",
            featureIcon: "cloud",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true }
        },{
            featureName: "Snapshot backups",
            featureIcon: "snapshot-backup",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true }
        },
    ];

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    Generate a one-time backup of this database.
                    <ul className="margin-top-xxs">
                        <li>Creating an ad-hoc backup may be essential before an upgrade or whenever an unscheduled backup is needed.</li>
                        <li>No retention period is defined for this backup, so it will not be automatically deleted.</li>
                    </ul>
                </div>
                <div>
                    Configuration options available:
                    <ul className="margin-top-xxs">
                        <li>Customize the backup type (Backup or Snapshot)</li>
                        <li>Specify destinations, where the backup file will be stored</li>
                        <li>Opt for backup data encryption to enhance data security</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={backupsDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Backups
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isProfessionalOrAbove}
                data={featureAvailabilityData}
            />
        </AboutViewFloating>
    );
}
