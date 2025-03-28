import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";

export interface ConnectionStringsLicenseFeatures {
    hasRavenEtl: boolean;
    hasSqlEtl: boolean;
    hasSnowflakeEtl: boolean;
    hasOlapEtl: boolean;
    hasElasticSearchEtl: boolean;
    hasQueueEtl: boolean;
    hasAiEtl: boolean;
}

interface ConnectionStringsLicense {
    features: ConnectionStringsLicenseFeatures;
    hasNone: boolean;
    hasAll: boolean;
}

export default function useConnectionStringsLicense(): ConnectionStringsLicense {
    const hasRavenEtl = useAppSelector(licenseSelectors.statusValue("HasRavenEtl"));
    const hasSqlEtl = useAppSelector(licenseSelectors.statusValue("HasSqlEtl"));
    const hasSnowflakeEtl = useAppSelector(licenseSelectors.statusValue("HasSnowflakeEtl"));
    const hasOlapEtl = useAppSelector(licenseSelectors.statusValue("HasOlapEtl"));
    const hasElasticSearchEtl = useAppSelector(licenseSelectors.statusValue("HasElasticSearchEtl"));
    const hasQueueEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasAiEtl = useAppSelector(licenseSelectors.statusValue("HasAiEtl"));

    const allFeatures = [
        hasRavenEtl,
        hasSqlEtl,
        hasSnowflakeEtl,
        hasOlapEtl,
        hasElasticSearchEtl,
        hasQueueEtl,
        hasAiEtl,
    ];
    const hasNone = allFeatures.every((x) => !x);
    const hasAll = allFeatures.every((x) => x);

    return {
        features: {
            hasRavenEtl,
            hasSqlEtl,
            hasSnowflakeEtl,
            hasOlapEtl,
            hasElasticSearchEtl,
            hasQueueEtl,
            hasAiEtl,
        },
        hasNone,
        hasAll,
    };
}
