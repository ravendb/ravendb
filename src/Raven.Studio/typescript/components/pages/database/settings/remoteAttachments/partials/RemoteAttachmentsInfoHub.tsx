import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export function RemoteAttachmentsInfoHub() {
    const hasRemoteAttachments = useAppSelector(licenseSelectors.statusValue("HasRemoteAttachments"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasRemoteAttachments,
            },
        ],
    });

    return (
        <AboutViewAnchored defaultOpen={hasRemoteAttachments ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <p>
                        To reduce local storage usage, attachments can be <strong>stored remotely</strong> instead of
                        being kept on your RavenDB server. The target storage destination and the desired upload time
                        must be specified when the attachment is added to a document.
                    </p>
                    <p>
                        When the <strong>Remote Attachments</strong> feature is enabled:
                    </p>
                    <ul>
                        <li>
                            RavenDB runs a background task that periodically scans the database for attachments marked
                            for remote upload and sends them to the configured remote storage destinations.
                        </li>
                        <li className="mt-1">
                            Uploads are handled in the background and do not block client operations.
                        </li>
                    </ul>
                    <p>In this view:</p>
                    <ul>
                        <li>Enable or disable the Remote Attachments feature.</li>
                        <li className="mt-1">
                            Configure remote destinations, including connection details and credentials. Supported
                            destinations include <strong>Azure Blob Storage</strong>, <strong>Amazon S3</strong>, or any
                            other <strong>S3-compatible</strong> storage service.
                        </li>
                        <li className="mt-1">
                            Fine-tune background processing behavior by setting:
                            <ul>
                                <li>The interval between background task runs</li>
                                <li>The maximum number of attachments processed per run</li>
                                <li>The number of attachments uploaded concurrently</li>
                            </ul>
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasRemoteAttachments} data={featureAvailability} />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Remote Attachments",
        featureIcon: "remote-attachment",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
