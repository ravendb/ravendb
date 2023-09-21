import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { useEnterpriseLicenseAvailability } from "components/utils/licenseLimitsUtils";

export function EditKafkaEtlInfoHub() {
    const isFeatureInLicense = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const featureAvailability = useEnterpriseLicenseAvailability(isFeatureInLicense);

    const kafkaEtlDocsLink = useRavenLink({ hash: "S45O2Y" });

    return (
        <AboutViewFloating defaultOpen={isFeatureInLicense ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    A <strong>Kafka ETL</strong> ongoing-task is an ETL (Extract, Transform & Load) process
                    that transfers data from this RavenDB database to topics of a Kafka broker.
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
                        Each script specifies the RavenDB source collection(s) and the destination topic(s).
                    </li>
                    <li className="margin-top-xxs">
                        The scripts are executed whenever documents in the source database are created or modified
                        (excluding deletes).
                    </li>
                    <li className="margin-top-xxs">
                        You can test each script in this view to preview the messages that will be sent by the ETL
                        task to the destination.
                    </li>
                </ul>
                <hr/>
                <div>
                    Task definition includes:
                    <ul>
                        <li>A connection string specifying Kafka&apos;s bootstrap servers.
                            <br/>
                            Connection options can be specified.
                        </li>
                        <li>The transformation scripts definitions.</li>
                        <li>Per topic, select whether processed documents will be deleted from your RavenDB database.
                        </li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={kafkaEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Kafka ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isFeatureInLicense}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}
