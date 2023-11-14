import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export function EditReplicationSinkInfoHub() {
    const hasPullReplicationAsSink = useAppSelector(licenseSelectors.statusValue("HasPullReplicationAsSink"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasPullReplicationAsSink,
            },
        ],
    });

    const replicationSinkDocsLink = useRavenLink({ hash: "MMUKGD" });

    return (
        <AboutViewFloating defaultOpen={hasPullReplicationAsSink ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    Define a <strong>Replication Sink</strong> task for opening a communication channel between this
                    database and a central Replication Hub that is located in another RavenDB cluster.
                    <ul className="margin-top-xxs">
                        <li>The connection to the Hub is always initiated by the Sink.</li>
                        <li>Multiple Sinks can connect to the same Replication Hub.</li>
                        <li>Data replication can be bi-directional.</li>
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
                            Set up a Replication Access definition on the Sink task with
                            a certificate that will be used by the Sink task to authenticate and
                            establish a connection with the Replication Hub.
                        </li>
                        <li className="margin-top-xxs"><strong>Filtered Replication</strong>:
                            <br/>
                            Within the Access definition, you can filter the documents that will be replicated by their ID paths.
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
                        <li>A connection string to the Replication Hub.</li>
                        <li>Setting the replication direction.</li>
                        <li>The Replication Access definition.</li>
                        <li>A responsible node to handle this task can be set.</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={replicationSinkDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Replication Sink
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasPullReplicationAsSink}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Replication Sink",
        featureIcon: "pull-replication-agent",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
