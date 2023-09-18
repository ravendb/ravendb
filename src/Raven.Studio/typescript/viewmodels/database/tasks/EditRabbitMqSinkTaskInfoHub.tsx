import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { featureAvailabilityEnterprise } from "components/utils/licenseLimitsUtils";
import React from "react";

export function EditRabbitMqSinkTaskInfoHub() {
    const isEnterpriseOrDeveloper = useAppSelector(licenseSelectors.isEnterpriseOrDeveloper);

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
                    Define a <strong>RabbitMQ Sink ongoing-task</strong> in order to consume and process incoming
                    messages from RabbitMQ queues.
                </p>
                <p>
                    Add one or more <strong>scripts</strong> that Load, Put, or Delete documents in RavenDB based on the
                    incoming messages.
                </p>
                <ul>
                    <li className="margin-top-sm">
                        In the task definition:
                        <br />
                        Define the connection string.
                    </li>
                    <li className="margin-top-xxs">
                        In the script definition:
                        <br />
                        Define the RabbitMQ queue the script will subscribe to.
                        <br />
                        The script can be tested to preview the resulting documents.
                    </li>
                    <li className="margin-top-xxs">Incoming messages are expected only as JSON.</li>
                </ul>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isEnterpriseOrDeveloper}
                data={featureAvailabilityEnterprise}
            />
        </AboutViewFloating>
    );
}
