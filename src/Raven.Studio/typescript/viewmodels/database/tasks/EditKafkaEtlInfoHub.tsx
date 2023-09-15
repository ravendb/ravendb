import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { featureAvailabilityEnterprise } from "components/utils/licenseLimitsUtils";

export function EditKafkaEtlInfoHub() {
    const isEnterpriseOrDeveloper = useAppSelector(licenseSelectors.isEnterpriseOrDeveloper());
    const kafkaEtlDocsLink = useRavenLink({ hash: "S45O2Y" });

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
                <a href={kafkaEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Kafka ETL
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isEnterpriseOrDeveloper}
                data={featureAvailabilityEnterprise}
            />
        </AboutViewFloating>
    );
}
