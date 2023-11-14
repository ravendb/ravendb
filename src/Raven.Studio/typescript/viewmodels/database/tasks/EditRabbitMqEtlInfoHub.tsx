import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export function EditRabbitMqEtlInfoHub() {
    const hasQueueEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasQueueEtl,
            },
        ],
    });

    const rabbitMqEtlDocsLink = useRavenLink({ hash: "KFKQM7" });

    return (
        <AboutViewFloating defaultOpen={hasQueueEtl ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    A <strong>RabbitMQ ETL</strong> ongoing-task is an ETL (Extract, Transform & Load) process
                    that transfers data from this RavenDB database to a RabbitMQ exchange.
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
                        Each script specifies the RavenDB source collection(s) and the destination Exchange(s).
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
                        <li>A connection string to the RabbitMQ exchange server.</li>
                        <li>The transformation scripts definitions.</li>
                        <li>Per queue, select whether processed documents will be deleted from your RavenDB database.
                        </li>
                        <li>When defining the RabbitMQ Exchanges, Queues & Bindings manually, 
                            you can request to skip their automatic creation.</li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={rabbitMqEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - RabbitMQ ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasQueueEtl}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "RabbitMQ ETL",
        featureIcon: "rabbitmq-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
