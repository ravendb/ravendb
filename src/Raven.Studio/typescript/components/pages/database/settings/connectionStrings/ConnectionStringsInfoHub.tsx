import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import useConnectionStringsLicense from "./useConnectionStringsLicense";
import appUrl from "common/appUrl";

export function ConnectionStringsInfoHub() {
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
                        <li>
                            RavenDB is designed to interact with diverse data storage solutions via replication, ETL, or
                            incoming data processing.
                        </li>
                        <li className="margin-top-xxs">
                            From this view, you can manage all the connection strings that may be used when defining an
                            ongoing-task per data storage.
                        </li>
                        <li className="margin-top-xxs">
                            New connection strings that have been created within an ongoing-task view will also be
                            listed here.
                        </li>
                        <li className="margin-top-xxs">
                            Connection strings that are in use by ongoing-tasks cannot be deleted, as they are essential
                            for task functionality and data access.
                        </li>
                        <li className="margin-top-xxs">
                            Connection strings can also be defined at the cluster level in the{" "}
                            <a href={appUrl.forServerWideConnectionStrings()} target="_blank">
                                Server-Wide Connection Strings view
                            </a>
                            . Those connection strings are propagated to all databases in the cluster, unless specific
                            databases are excluded.
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
