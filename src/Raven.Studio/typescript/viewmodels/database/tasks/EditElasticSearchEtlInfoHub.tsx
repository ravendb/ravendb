import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { featureAvailabilityEnterprise } from "components/utils/licenseLimitsUtils";

export function EditElasticSearchEtlInfoHub() {
    const isEnterpriseOrDeveloper = useAppSelector(licenseSelectors.isEnterpriseOrDeveloper);
    const elasticSearchEtlDocsLink = useRavenLink({ hash: "AHPBTX" });

    return (
        <AboutViewFloating defaultOpen={isEnterpriseOrDeveloper ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    An <strong>Elasticsearch ETL</strong> ongoing-task is an ETL (Extract, Transform & Load) process
                    that transfers data from documents from this RavenDB database to an Elasticsearch destination.
                </p>
                <ul>
                    <li>The sent data can be filtered and modified by multiple transformation JavaScript scripts that
                        are added to the task.
                    </li>
                    <li className="margin-top-xxs">
                        An Elasticsearch ETL task transfers documents only.
                        <br/>
                        Attachments, Counters, Time series, and Revisions are not transferred.
                    </li>
                    <li className="margin-top-xxs">
                        Each script specifies the RavenDB source collection(s) and the destination Elasticsearch
                        index(es).
                    </li>
                    <li className="margin-top-xxs">
                        The scripts are executed whenever documents in the source database are created, modified, or
                        deleted.
                    </li>
                    <li className="margin-top-xxs">
                        You can test each script in this view to preview the commands the ETL task would send
                        Elasticsearch.
                    </li>
                </ul>
                <hr/>
                <div>
                    Task definition includes:
                    <ul>
                        <li>The transformation scripts definitions.</li>
                        <li>List of Elasticsearch indexes the ETL task will access, including the property
                            used for the RavenDB document ID.
                        </li>
                        <li>A connection string with URLs to the Elasticsearch nodes and the authentication method.
                        </li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={elasticSearchEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Elasticsearch ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isEnterpriseOrDeveloper}
                data={featureAvailabilityEnterprise}
            />
        </AboutViewFloating>
    );
}
