import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import {Icon} from "components/common/Icon";

export function EditPeriodicBackupTaskInfoHub() {
    const licenseType = useAppSelector(licenseSelectors.licenseType);

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
                    Define an ongoing-task that will automatically create periodic backups for this database at the defined <strong>schedule</strong>.
                </p>
                <p>
                    Configuration options available:
                    <ul>
                        <li>Customize the backup type (Backup or Snapshot)</li>
                        <li>Select full and/or incremental backups</li>
                        <li>Set the backups retention period</li>
                        <li>Specify destinations, where the backup files will be stored</li>
                        <li>Opt for backup data encryption to enhance data security</li>
                    </ul>
                </p>
                <p>
                    In addition:
                    <ul>
                        <li>The task state can be disabled as needed</li>
                        <li>You can define a specific node to handle this task</li>
                    </ul>
                </p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href="https://ravendb.net/l/GMBYOH/6.0/Csharp" target="_blank">
                    <Icon icon="newtab" /> Docs - Backups
                </a>
            </AccordionItemWrapper>
            {licenseType === "Community" && (
                <AccordionLicenseNotIncluded
                    targetId="licensing"
                    featureName="Periodic Backups"
                    featureIcon="periodic-backup"
                    checkedLicenses={["Professional", "Enterprise"]}
                />
            )}
        </AboutViewFloating>
    );
}
