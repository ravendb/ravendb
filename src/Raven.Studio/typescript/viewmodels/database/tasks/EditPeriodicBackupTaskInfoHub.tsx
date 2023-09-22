import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {FeatureAvailabilityData} from "components/common/FeatureAvailabilitySummary";
import {
    useLimitedFeatureAvailability
} from "components/utils/licenseLimitsUtils";

export function EditPeriodicBackupTaskInfoHub() {
    const hasPeriodicBackups = useAppSelector(licenseSelectors.statusValue("HasPeriodicBackup"));
    const hasSnapshotBackups = useAppSelector(licenseSelectors.statusValue("HasSnapshotBackups"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasPeriodicBackups,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasSnapshotBackups,
            }
        ],
    });

    const backupsDocsLink = useRavenLink({ hash: "GMBYOH" });

    return (
        <AboutViewFloating defaultOpen={hasPeriodicBackups ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    Define an ongoing-task that will automatically create periodic backups for this database at the
                    defined <strong>schedule</strong>.
                </p>
                <div>
                    Configuration options available:
                    <ul>
                        <li>Customize the backup type (Backup or Snapshot)</li>
                        <li>Select full and/or incremental backups</li>
                        <li>Set the backups retention period</li>
                        <li>Specify destinations, where the backup files will be stored</li>
                        <li>Opt for backup data encryption to enhance data security</li>
                    </ul>
                </div>
                <div>
                    In addition:
                    <ul>
                        <li>The task state can be disabled as needed</li>
                        <li>You can set a responsible node to handle this task</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={backupsDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Backups
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasPeriodicBackups && hasSnapshotBackups}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Periodic Backups",
        featureIcon: "periodic-backup",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true }
    },{
        featureName: "Snapshot Backups",
        featureIcon: "snapshot-backup",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true }
    }
];
