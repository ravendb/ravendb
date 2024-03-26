import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";

export function IntegrationsInfoHub() {
    const hasPostgreSql = useAppSelector(licenseSelectors.statusValue("HasPostgreSqlIntegration"));
    const hasPowerBi = useAppSelector(licenseSelectors.statusValue("HasPowerBI"));

    const integrationsDocsLink = useRavenLink({ hash: "ZE1BYH" });

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasPostgreSql,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasPowerBi,
            },
        ],
    });

    const hasAllFeaturesInLicense = hasPostgreSql && hasPowerBi;

    return (
        <AboutViewAnchored defaultOpen={hasAllFeaturesInLicense ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    In this view, you can define credentials that third-party clients are required to provide when
                    connecting to RavenDB.
                </p>
                <p>
                    Providing the credentials is only required of clients when RavenDB is installed as a Secure Server.
                </p>
                <hr />
                <div>
                    PostgreSQL support:
                    <ul>
                        <li className="margin-top-xxs">
                            RavenDB implements the PostgreSQL protocol, allowing clients that use PostgreSQL (e.g. Power
                            BI) to retrieve data from a RavenDB database.
                        </li>
                        <li className="margin-top-xxs">
                            Verify your license supports the PostgreSQL Protocol.
                            <br />
                            The PostgreSQL protocol support must also be explicitly enabled in your RavendB&apos;s
                            settings.
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={integrationsDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Integrations
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasAllFeaturesInLicense} data={featureAvailability} />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "PostgreSQL",
        featureIcon: "postgresql",
        community: { value: true },
        professional: { value: true },
        enterprise: { value: true },
    },
    {
        featureName: "Power BI",
        featureIcon: "powerbi",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
    },
];
