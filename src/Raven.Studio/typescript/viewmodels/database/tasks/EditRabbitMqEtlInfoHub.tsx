import AboutViewFloating, { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { featureAvailabilityEnterprise } from "components/utils/licenseLimitsUtils";

export function EditRabbitMqEtlInfoHub() {
    const isEnterpriseOrDeveloper = useAppSelector(licenseSelectors.isEnterpriseOrDeveloper());
    const rabbitMqEtlDocsLink = useRavenLink({ hash: "KFKQM7" });

    return (
        <AboutViewFloating defaultOpen={isEnterpriseOrDeveloper ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>Text</p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={rabbitMqEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - RabbitMQ ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isEnterpriseOrDeveloper}
                data={featureAvailabilityEnterprise}
            />
        </AboutViewFloating>
    );
}
