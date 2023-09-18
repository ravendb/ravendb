import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { featureAvailabilityEnterprise } from "components/utils/licenseLimitsUtils";
import {useRavenLink} from "hooks/useRavenLink";

export function DocumentCompressionInfoHub() {
    const isEnterpriseOrDeveloper = useAppSelector(licenseSelectors.isEnterpriseOrDeveloper);
    const compressionDocsLink = useRavenLink({ hash: "E2WX16" });

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
                    Reduce the database storage space by enabling document compression from this view.
                </p>
                <div>
                    Document compression can be set for:
                    <ul>
                        <li>Documents in all collections or selected collections.</li>
                        <li>Revisions in all collections.</li>
                    </ul>
                </div>
                <p>
                    Compression is not applied to attachments, counters, and time series data,
                    only to the content of documents and revisions.
                </p>
                <div>
                    When enabled, compression will be triggered by the server when either of the following occurs for
                    the configured collections:
                    <ul>
                        <li>Storing new documents.</li>
                        <li>Modifying & saving existing documents.</li>
                        <li>When a compact operation is initiated by the Client API, existing documents will be compressed.
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={compressionDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Document Compression
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isEnterpriseOrDeveloper}
                data={featureAvailabilityEnterprise}
            />
        </AboutViewFloating>
    );
}
