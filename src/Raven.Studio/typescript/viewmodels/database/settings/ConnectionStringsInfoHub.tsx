import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {FeatureAvailabilityData} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";

export function ConnectionStringsInfoHub() {
    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove);
    const featureAvailabilityData: FeatureAvailabilityData[] = [
        {
            featureName: "RavenDB ETL",
            featureIcon: "ravendb-etl",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
        },{
            featureName: "SQL ETL",
            featureIcon: "sql-etl",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
        },
        {
            featureName: "OLAP ETL",
            featureIcon: "olap-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
        },
        {
            featureName: "Elasticsearch ETL",
            featureIcon: "elastic-search-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
        },
        {
            featureName: "Kafka ETL",
            featureIcon: "kafka-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
        },
        {
            featureName: "RabbitMQ ETL",
            featureIcon: "rabbitmq-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
        },
    ];

    return (
        <AboutViewFloating defaultOpen={isProfessionalOrAbove ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>Text</p>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isProfessionalOrAbove}
                data={featureAvailabilityData}
            />
        </AboutViewFloating>
    );
}
