import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import useConnectionStringsLicense from "components/pages/database/settings/connectionStrings/useConnectionStringsLicense";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";

export default function ServerWideConnectionStringsInfoHub() {
    const { hasAll, featureAvailability } = useConnectionStringsLicense();
    const hasServerWideConnectionStrings = useAppSelector(
        licenseSelectors.statusValue("HasServerWideConnectionStrings")
    );

    const serverWideFeatureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasServerWideConnectionStrings,
            },
        ],
    });

    const isLicenseUnlimited = hasAll && hasServerWideConnectionStrings;

    const connectionStringsOverviewDocsLink = useRavenLink({ hash: "P5XJOV" });
    const connectionStringsServerwideDocsLink = useRavenLink({ hash: "5AQ7XL" });

    return (
        <AboutViewAnchored defaultOpen={isLicenseUnlimited ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <div>
                    <ul>
                        <li>Use this view to manage server-wide connection strings at the cluster level.</li>
                        <li className="margin-top-xxs">
                            <strong>Server-wide connection strings</strong> are automatically propagated to ALL
                            databases in the cluster, unless specific databases are excluded.
                        </li>
                        <li className="margin-top-xxs">
                            Ongoing tasks defined on a database can use the server-wide connection strings that are
                            available to that database.
                        </li>
                        <li className="margin-top-xxs">
                            Connection strings that are used by ongoing tasks cannot be deleted.
                        </li>
                        <li className="margin-top-xxs">
                            Server-wide connection strings are not included when exporting a database or restoring it
                            from backup.
                        </li>
                    </ul>
                    <hr />
                    <div className="small-label mb-2">useful links</div>
                    <a href={connectionStringsOverviewDocsLink} target="_blank">
                        <Icon icon="newtab" /> Docs - Connection Strings Overview
                    </a>
                    <br />
                    <a href={connectionStringsServerwideDocsLink} target="_blank">
                        <Icon icon="newtab" /> Docs - Connection Strings Server-wide
                    </a>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                data={[...serverWideFeatureAvailability, ...featureAvailability]}
                isUnlimited={isLicenseUnlimited}
                isOpenedByDefault={!hasServerWideConnectionStrings}
            />
        </AboutViewAnchored>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Server-Wide Connection Strings",
        featureIcon: "manage-connection-strings",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
