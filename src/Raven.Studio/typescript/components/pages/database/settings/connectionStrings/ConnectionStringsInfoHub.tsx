import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import useConnectionStringsLicense from "./useConnectionStringsLicense";

export function ConnectionStringsInfoHub() {
    const { hasAll, features } = useConnectionStringsLicense();

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "ravendb-etl").featureName,
                value: features.hasRavenEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "sql-etl").featureName,
                value: features.hasSqlEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "snowflake-etl").featureName,
                value: features.hasSnowflakeEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "olap-etl").featureName,
                value: features.hasOlapEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "elastic-search-etl").featureName,
                value: features.hasElasticSearchEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "kafka-etl").featureName,
                value: features.hasQueueEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "rabbitmq-etl").featureName,
                value: features.hasQueueEtl,
            },
            {
                featureName: defaultFeatureAvailability.find((x) => x.featureIcon === "azure-queue-storage-etl")
                    .featureName,
                value: features.hasQueueEtl,
            },
        ],
    });

    return (
        <AboutViewAnchored defaultOpen={hasAll ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    <ul>
                        <li>
                            RavenDB is designed to interact with diverse data storage solutions via replication, ETL, or
                            incoming data processing.
                        </li>
                        <li className="margin-top-xxs">
                            From this view, you can manage all the connection strings that may be used when defining an
                            ongoing-task per data storage.
                        </li>
                        <li className="margin-top-xxs">
                            New connection strings that have been created within an ongoing-task view will also be
                            listed here.
                        </li>
                        <li className="margin-top-xxs">
                            Connection strings that are in use by ongoing-tasks cannot be deleted, as they are essential
                            for task functionality and data access.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasAll} data={featureAvailability} />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "RavenDB ETL",
        featureIcon: "ravendb-etl",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
    {
        featureName: "SQL ETL",
        featureIcon: "sql-etl",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
    {
        featureName: "Snowflake ETL",
        featureIcon: "snowflake-etl",
        community: { value: false },
        professional: { value: false },
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
    {
        featureName: "Azure Queue Storage ETL",
        featureIcon: "azure-queue-storage-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
