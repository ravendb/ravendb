import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { featureAvailabilityEnterprise } from "components/utils/licenseLimitsUtils";
import {useRavenLink} from "hooks/useRavenLink";

export function EditReplicationHubInfoHub() {
    const isEnterpriseOrDeveloper = useAppSelector(licenseSelectors.isEnterpriseOrDeveloper);
    const replicationHubDocsLink = useRavenLink({ hash: "NIH5LN" });

    return (
        <AboutViewFloating defaultOpen={isEnterpriseOrDeveloper ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    Define a <strong>Replication Hub</strong> to replicate documents to and/or from multiple Replication
                    Sink tasks that are defined in other RavenDB databases in other clusters:
                    <ul className="margin-top-xxs">
                        <li>A Replication Hub can serve many Sinks.</li>
                        <li>The connection to the Hub is always initiated by the Sink.</li>
                        <li>Only documents and their related data (attachments, revisions, counters & time series) will
                            be replicated.
                        </li>
                    </ul>
                </div>
                <hr/>
                <div>
                    When running with a SECURE server:
                    <ul>
                        <li className="margin-top-xxs"><strong>Replication Access</strong>:
                            <br/>
                            You can set up multiple Replication Access definitions on the Hub task with
                            an associated certificate, which will be used by the Sink tasks to authenticate and
                            establish a connection with the Replication Hub.
                        </li>
                        <li className="margin-top-xxs"><strong>Filtered Replication</strong>:
                            <br/>
                            Within each Access definition, you can filter the documents that will be replicated by their
                            ID paths.
                        </li>
                        <li className="margin-top-xxs">
                            <strong>Sink to Hub Replication</strong> is only available with a secure server.
                        </li>
                    </ul>
                </div>
                <hr/>
                <div>
                    Task definition includes:
                    <ul className="margin-top-xxs">
                        <li>Setting the replication direction.</li>
                        <li>The replication access definitions.</li>
                        <li>Control whether deletions from Sink are executed on the Hub.</li>
                        <li>An optional delay time for data replication.</li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={replicationHubDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Replication Hub
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isEnterpriseOrDeveloper}
                data={featureAvailabilityEnterprise}
            />
        </AboutViewFloating>
    );
}
