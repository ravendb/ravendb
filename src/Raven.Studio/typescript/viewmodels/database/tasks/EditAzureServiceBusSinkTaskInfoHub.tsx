import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import FeatureAvailabilitySummaryWrapper, { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";

export function EditAzureServiceBusSinkTaskInfoHub() {
    const hasQueueSink = useAppSelector(licenseSelectors.statusValue("HasQueueSink"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasQueueSink,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasQueueSink ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
            >
                <p>
                    Define an <strong>Azure Service Bus Sink</strong> ongoing-task in order to consume and process incoming messages from Azure Service Bus queues.
                </p>
                <p>
                    Add one or more <strong>scripts</strong> that Load, Put, or Delete documents in RavenDB based on the incoming messages:
                </p>
                <ul>
                    <li className="margin-top-sm">
                        In the task definition:<br />
                        Define the connection string (connection string and queue name).
                    </li>
                    <li className="margin-top-xxs">
                        In the script definition:<br />
                        Define the Azure Service Bus queue the script will subscribe to.<br />
                        The script can be tested to preview the resulting documents.
                    </li>
                    <li className="margin-top-xxs">
                        Incoming messages are expected only as JSON.
                    </li>
                </ul>
                <p>
                    A database with a defined Azure Service Bus Sink task will Not become idle,<br />
                    ensuring continuous processing of incoming messages.
                </p>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasQueueSink}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Azure Service Bus Sink",
        featureIcon: "azure-service-bus",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
