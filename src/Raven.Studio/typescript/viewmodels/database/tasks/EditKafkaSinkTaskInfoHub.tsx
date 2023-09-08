import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";

export function EditKafkaSinkTaskInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove());

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    Define a <strong>Kafka Sink ongoing-task</strong> in order to consume and process incoming messages from Kafka topics.
                </p>
                <p>
                    Add one or more <strong>scripts</strong> that Load, Put, or Delete documents in RavenDB based on the incoming messages.
                </p>
                <ul>
                    <li className="margin-top-sm">
                        In the task definition:<br />
                        Define the connection string (bootstrap servers and any additional connection options).
                    </li>
                    <li className="margin-top-xxs">
                        In the script definition:<br />
                        Define the Kafka topic the script will subscribe to.<br />
                        The script can be tested to preview the resulting documents.
                    </li>
                    <li className="margin-top-xxs">
                        Incoming messages are expected only as JSON.
                    </li>
                </ul>
            </AccordionItemWrapper>
            <AccordionLicenseNotIncluded
                targetId="licensing"
                featureName="Kafka Sink"
                featureIcon="kafka-sink"
                checkedLicenses={["Professional", "Enterprise"]}
                isLimited={!isProfessionalOrAbove}
            />
        </AboutViewFloating>
    );
}
