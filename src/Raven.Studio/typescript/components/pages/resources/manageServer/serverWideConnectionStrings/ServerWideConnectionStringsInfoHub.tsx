import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import useConnectionStringsLicense from "components/pages/database/settings/connectionStrings/useConnectionStringsLicense";

export default function ServerWideConnectionStringsInfoHub() {
    const { hasAll, featureAvailability } = useConnectionStringsLicense();

    return (
        <AboutViewAnchored defaultOpen={hasAll ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    <ul>
                        <li>Use this view to manage server-wide connection strings at the cluster level.</li>
                        <li className="margin-top-xxs">
                            <strong>Server-wide connection strings</strong> are automatically propagated to ALL
                            databases in the cluster, unless specific databases are excluded.
                        </li>
                        <li className="margin-top-xxs">
                            Ongoing tasks defined on a database can use the server-wide connection strings that are
                            available to that database.
                        </li>
                        <li className="margin-top-xxs">
                            Connection strings that are used by ongoing tasks cannot be deleted.
                        </li>
                        <li className="margin-top-xxs">
                            Server-wide connection strings are not included when exporting a database or restoring it
                            from backup.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                data={featureAvailability}
                isUnlimited={hasAll}
                isOpenedByDefault={false}
            />
        </AboutViewAnchored>
    );
}
