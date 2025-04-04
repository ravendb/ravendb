import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {FeatureAvailabilityData} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import {useRavenLink} from "hooks/useRavenLink";
import {Icon} from "components/common/Icon";

export function CertificatesInfoHub() {
    const hasReadOnlyCertificates = useAppSelector(licenseSelectors.statusValue("HasReadOnlyCertificates"));
    const certificatesDocsLink = useRavenLink({ hash: "S3G2T1" });

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasReadOnlyCertificates,
            },
        ],
    });

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
                    Manage your server and client certificates from this view.
                </p>
                <div>
                    <strong>Server certificates</strong>:
                    <ul>
                        <li className="margin-top-xxs">
                            The server certificate can be exported for the purpose of uploading it to another server,
                            in order to establish a secure connection between the servers.
                        </li>
                        <li className="margin-top-xxs">
                            The server certificate can be replaced as needed - this will apply to all cluster nodes.
                        </li>
                    </ul>
                </div>
                <div>
                    <strong>Client certificates:</strong>
                    <ul>
                        <li className="margin-top-xxs">
                            An admin can generate client certificates with an optional passphrase, and an expiration
                            period,
                            specifying the security clearance (Cluster Admin / Operator / User)
                        </li>
                        <li className="margin-top-xxs">
                            With the User security clearance, different access permissions
                            <br/>
                            (Admin / Read-Write / Read-Only) can be granted per database.
                        </li>
                        <li className="margin-top-xxs">
                            You can upload a client certificate that was exported from another server,
                            allowing access to users with existing certificates.
                        </li>
                        <li className="margin-top-xxs">
                            RavenDB does not keep track of the client certificate&apos;s private key.
                        </li>
                    </ul>
                </div>
                <hr/>
                <div className="small-label mb-2">useful links</div>
                <a href={certificatesDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Manage Certificates
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasReadOnlyCertificates}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Read-Only Certificates",
        featureIcon: "access-read",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    }
];
