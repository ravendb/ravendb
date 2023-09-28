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

export function EditServerWideBackupInfoHub() {
    const hasServerWideBackups = useAppSelector(licenseSelectors.statusValue("HasServerWideBackups"));
    const hasSnapshotBackups = useAppSelector(licenseSelectors.statusValue("HasSnapshotBackups"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasServerWideBackups,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasSnapshotBackups,
            }
        ],
    });

    const backupTasksDocsLink = useRavenLink({ hash: "SXSM33" });

    return (
        <AboutViewFloating defaultOpen={hasServerWideBackups ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    Defining a <strong>Server-Wide backup task</strong> will create an ongoing periodic backup task for
                    each database in your cluster.
                    <ul className="margin-top-xxs">
                        <li>You can select specific databases to exclude from the task.</li>
                        <li>
                            The configurations set in the Server-Wide task will be applied to the corresponding ongoing
                            backup task created per database.
                        </li>
                    </ul>
                </div>
                <div>
                    Configuration options available:
                    <ul className="margin-top-xxs">
                        <li>Customize the backup type (Backup or Snapshot)</li>
                        <li>Select full and/or incremental backups</li>
                        <li>Set the backups retention period</li>
                        <li>Specify destinations, where the backup files will be stored</li>
                        <li>Opt for backup data encryption to enhance data security</li>
                    </ul>
                </div>
                <div>
                    In addition:
                    <ul className="margin-top-xxs">
                        <li>You can set a responsible node to handle this task.</li>
                        <li>Disabling this task will disable the corresponding tasks per database.</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={backupTasksDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Server-Wide Backup Task
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasServerWideBackups && hasSnapshotBackups}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Server-Wide Backups",
        featureIcon: "server-wide-backup",
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
