import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export function EditAzureQueueStorageEtlInfoHub() {
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

    //TODO: update docs link
    const azureQueueStorageEtlDocsLink = useRavenLink({ hash: "TODO" }); 

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
                    A <strong>Azure Queue Storage ETL</strong> ongoing-task is an ETL (Extract, Transform & Load) process
                    that transfers data from this RavenDB database to a Azure Queue Storage.
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
                        Each script specifies the RavenDB source collection(s) and the destination queues(s).
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
                        <li>A connection string to the Azure Queue Storage server.</li>
                        <li>The transformation scripts definitions.</li>
                        <li>Per queue, select whether processed documents will be deleted from your RavenDB database.
                        </li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr />
                {
                /* TODO
                <div className="small-label mb-2">useful links</div>    
                <a href={azureQueueStorageEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Azure Queue Storage ETL
                </a>
                 */}
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
        featureName: "Azure Queue Storage ETL",
        featureIcon: "azure-queue-storage-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
