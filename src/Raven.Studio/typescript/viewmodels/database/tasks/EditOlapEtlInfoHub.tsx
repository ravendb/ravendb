import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export function EditOlapEtlInfoHub() {
    const hasOlapEtl = useAppSelector(licenseSelectors.statusValue("HasOlapEtl"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasOlapEtl,
            },
        ],
    });

    const olapEtlDocsLink = useRavenLink({ hash: "LYZL56" });

    return (
        <AboutViewFloating defaultOpen={hasOlapEtl ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    An <strong>OLAP ETL</strong> ongoing-task is an ETL (Extract, Transform & Load) process that
                    converts documents&apos; data from this RavenDB database to the Apache Parquet file format, and sends
                    it to the specified destinations.
                </p>
                <ul>
                    <li>
                        Data is extracted from documents only.
                        <br/>
                        Attachments, Counters, Time series, and Revisions are not sent.
                    </li>
                    <li className="margin-top-xxs">
                        The sent data can be filtered and modified by multiple transformation JavaScript scripts that
                        are added to the task.
                    </li>
                    <li className="margin-top-xxs">
                        Each script specifies the RavenDB source collection(s) and the folders & partitions for the
                        generated Parquet file.
                    </li>
                    <li className="margin-top-xxs">
                        The scripts are executed only at the specified time intervals, processing only new or modified
                        documents (excluding deletes).
                    </li>
                    <li className="margin-top-xxs">
                        You can test each script in this view to preview the columnar data that will be sent by the ETL
                        task to the destination.
                    </li>
                </ul>
                <hr/>
                <div>
                    Task definition includes:
                    <ul>
                        <li>The frequency at which scripts will execute.</li>
                        <li>A connection string specifying the storage destinations.</li>
                        <li>The transformation scripts definitions.</li>
                        <li>Optional - a column name that will override the default ID column name in the generated
                            Parquet file.
                        </li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={olapEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - OLAP ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasOlapEtl}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "OLAP ETL",
        featureIcon: "olap-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
