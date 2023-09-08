import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import {Icon} from "components/common/Icon";

export function EditServerWideBackupInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove());

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    Defining a <strong>Server-Wide backup task</strong> will create an ongoing periodic backup task for each database in your cluster.
                    <ul className="margin-top-xs">
                        <li>You can select specific databases to exclude from the task.</li>
                        <li>The configurations set in the Server-Wide task will be applied to the corresponding ongoing backup task created per database.</li>
                    </ul>
                </p>
                <p>
                    Configuration options available:
                    <ul className="margin-top-xs">
                        <li>Customize the backup type (Backup or Snapshot)</li>
                        <li>Select full and/or incremental backups</li>
                        <li>Set the backups retention period</li>
                        <li>Specify destinations, where the backup files will be stored</li>
                        <li>Opt for backup data encryption to enhance data security</li>
                    </ul>
                </p>
                <p>
                    In addition:
                    <ul className="margin-top-xs">
                        <li>You can set a responsible node to handle this task.</li>
                        <li>Disabling this task will disable the corresponding tasks per database.</li>
                    </ul>
                </p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href="https://ravendb.net/l/SXSM33/latest" target="_blank">
                    <Icon icon="newtab" /> Docs - Server-Wide Backup Task
                </a>
            </AccordionItemWrapper>
            <AccordionLicenseNotIncluded
                targetId="licensing"
                featureName="Server-Wide Backups"
                featureIcon="server-wide-backup"
                checkedLicenses={["Professional", "Enterprise"]}
                isLimited={!isProfessionalOrAbove}
            />
        </AboutViewFloating>
    );
}
