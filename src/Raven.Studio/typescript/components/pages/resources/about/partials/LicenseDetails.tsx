import { Button, Col, Input, Row, Table } from "reactstrap";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { RadioToggleWithIcon, RadioToggleWithIconInputItem } from "components/common/RadioToggle";
import classNames from "classnames";
import { aboutPageUrls } from "components/pages/resources/about/partials/common";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";

export function LicenseDetails() {
    const licenseId = useAppSelector(licenseSelectors.statusValue("Id"));
    const licenseTo = useAppSelector(licenseSelectors.statusValue("LicensedTo"));
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    return (
        <React.Fragment key="license-details">
            <div className="bg-faded-primary mb-4">
                {licenseType !== "None" ? (
                    <Row className="text-center py-4">
                        <Col className="px-4">
                            <div className="small text-muted">License ID</div>
                            <h4 className="fw-bolder text-emphasis m-0">{licenseId}</h4>
                        </Col>
                        <Col className="px-4">
                            <div className="small text-muted">License To</div>
                            <h4 className="fw-bolder text-emphasis m-0">{licenseTo}</h4>
                        </Col>
                    </Row>
                ) : (
                    <div className="text-center py-4">
                        <h3 className="fw-bolder text-warning d-flex align-items-center justify-content-center">
                            <Icon icon="empty-set" className="fs-1" margin="me-3" /> No license - AGPLv3 Restrictions
                            Applied
                        </h3>
                        <Button
                            color="success"
                            className="px-4 rounded-pill"
                            size="lg"
                            href={aboutPageUrls.getLicense}
                            target="_blank"
                        >
                            <strong>Get free license</strong>
                            <Icon icon="newtab" margin="ms-2" />
                        </Button>
                    </div>
                )}
            </div>

            <LicenseTable licenseType={licenseType} />
        </React.Fragment>
    );
}

interface LicenseTableProps {
    licenseType: Raven.Server.Commercial.LicenseType;
}

function LicenseTable(props: LicenseTableProps) {
    const { licenseType } = props;

    const [searchText, setSearchText] = useState("");
    const [viewMode, setViewMode] = useState<"showDiff" | "showAll">("showAll");

    const licenseStatus = useAppSelector(licenseSelectors.status);
    const { columns, current: currentColumn } = getColumns(licenseType);

    const upgradeLicenseBtnHandler = () => {
        window.open(`https://ravendb.net/buy`, "_blank");
    };

    const getEffectiveValue = (feature: FeatureAvailabilityItem, column: LicenseColumn) => {
        if (column !== currentColumn || !feature.fieldInLicense) {
            return feature[column].value;
        }

        const licenseValue = licenseStatus[feature.fieldInLicense];

        if (licenseValue === null) {
            return Infinity;
        }

        return licenseValue;
    };

    const filteredSections = filterFeatureAvailabilitySection(
        featureAvailabilityData,
        searchText,
        viewMode,
        columns,
        getEffectiveValue
    );

    const onSearchTextChange = (searchText: string) => {
        setSearchText(searchText);
    };

    const leftRadioToggleItem: RadioToggleWithIconInputItem<"showDiff"> = {
        label: "Show differences",
        value: "showDiff",
        iconName: "diff",
    };

    const rightRadioToggleItem: RadioToggleWithIconInputItem<"showAll"> = {
        label: "Show all license properties",
        value: "showAll",
        iconName: "license",
    };

    const showUpgradeButton = licenseType !== "Enterprise";

    return (
        <>
            <div className="px-4 pb-4">
                <div className="clearable-input">
                    <Input
                        type="text"
                        accessKey="/"
                        placeholder="Filter: e.g. ETL"
                        title="Filter indexes"
                        className="filtering-input"
                        value={searchText}
                        onChange={(e) => onSearchTextChange(e.target.value)}
                    />
                    {searchText && (
                        <div className="clear-button">
                            <Button color="secondary" size="sm" onClick={() => onSearchTextChange("")}>
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            <div className="table-responsive license-feature-availability-table">
                <Table borderless hover striped className="text-center feature-availability-table">
                    <thead>
                        <tr>
                            <th>License type</th>
                            {columns.map((column) => (
                                <th key={column} className={classNames({ "bg-current": column === currentColumn })}>
                                    <h4 className="fw-bolder text-uppercase m-0">
                                        {column === "community" && licenseType === "Essential"
                                            ? "Essential"
                                            : column.toLocaleUpperCase()}
                                    </h4>
                                    {column === currentColumn && <div className="text-primary">Current</div>}
                                </th>
                            ))}
                        </tr>
                        {showUpgradeButton && (
                            <tr>
                                <th></th>
                                <th className={classNames({ "bg-current": columns.length !== 4 })}></th>
                                <th colSpan={columns.length < 4 ? columns.length - 1 : 2} className="px-3">
                                    <Button
                                        color="primary"
                                        className="w-100 rounded-pill"
                                        onClick={upgradeLicenseBtnHandler}
                                    >
                                        <Icon icon="upgrade-arrow" />
                                        Upgrade license
                                    </Button>
                                </th>
                                {columns.length >= 4 && <th className="bg-current"></th>}
                            </tr>
                        )}
                    </thead>
                    <tbody>
                        {filteredSections.length === 0 && (
                            <tr>
                                <td>No matches found</td>
                            </tr>
                        )}
                        {filteredSections.map((section) => (
                            <React.Fragment key={section.name ?? "n/a"}>
                                {section.name && (
                                    <tr key={section.name}>
                                        <React.Fragment key={section.name}>
                                            <th scope="row">
                                                {section.link ? (
                                                    <a href={section.link} target="_blank" className="fw-bold">
                                                        {section.name} <Icon icon="newtab" margin="m-0" />
                                                    </a>
                                                ) : (
                                                    <div className="fw-bold text-light">{section.name}</div>
                                                )}
                                            </th>
                                            {columns.map((column) => (
                                                <td
                                                    key={column}
                                                    className={classNames({
                                                        "bg-current": column === currentColumn,
                                                    })}
                                                ></td>
                                            ))}
                                        </React.Fragment>
                                    </tr>
                                )}

                                {section.items.map((feature) => (
                                    <tr key={feature.name}>
                                        <th scope="row">{feature.name}</th>
                                        {columns.map((column) => (
                                            <td
                                                key={column}
                                                className={classNames({
                                                    "bg-current": column === currentColumn,
                                                })}
                                            >
                                                <FeatureValue
                                                    value={getEffectiveValue(feature, column)}
                                                    suffix={feature.suffix}
                                                />
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                            </React.Fragment>
                        ))}
                    </tbody>
                </Table>
            </div>
            {columns.length > 1 && (
                <div className="p-2">
                    <div className="well rounded-pill text-center p-1">
                        <RadioToggleWithIcon<"showDiff" | "showAll">
                            name="some-name"
                            leftItem={leftRadioToggleItem}
                            rightItem={rightRadioToggleItem}
                            selectedValue={viewMode}
                            setSelectedValue={(x) => setViewMode(x)}
                        />
                    </div>
                </div>
            )}
        </>
    );
}

const availableEverywhere = {
    community: { value: true },
    professional: { value: true },
    enterprise: { value: true },
    developer: { value: true },
};

const featureAvailabilityData: FeatureAvailabilitySection[] = [
    {
        name: null,
        link: null,
        items: [
            {
                name: "Eligible for Commercial Use",
                fieldInLicense: null,
                ...availableEverywhere,
            },
            {
                name: "Number of databases",
                community: { value: Infinity },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: null,
            },
            {
                name: "Single database size",
                community: { value: Infinity },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: null,
            },
            {
                name: "Management Studio (GUI)",
                ...availableEverywhere,
                fieldInLicense: null,
            },
        ],
    },
    {
        name: "Clustering",
        link: "https://ravendb.net/features#clusters",
        items: [
            {
                name: "Max cluster size",
                community: { value: "3" },
                professional: { value: "5" },
                enterprise: { value: Infinity },
                developer: { value: "3" },
                fieldInLicense: "MaxClusterSize",
                suffix: "nodes",
            },
            {
                name: "Max cores in cluster",
                community: { value: 3 },
                professional: { value: 40 },
                enterprise: { value: Infinity },
                developer: { value: 9 },
                fieldInLicense: "MaxCores",
            },
            {
                name: "Max cluster memory usage",

                community: { value: "6" },
                professional: { value: "240" },
                enterprise: { value: Infinity },
                developer: { value: "36" },
                fieldInLicense: "MaxMemory",
                suffix: "GB RAM",
            },
            {
                name: "Cluster Dashboard",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Cluster transactions",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Highly available tasks",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasHighlyAvailableTasks",
            },
            {
                name: "Dynamic database distribution",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasDynamicNodesDistribution",
            },
        ],
    },
    {
        name: "Indexes",
        link: "https://ravendb.net/features#indexes",
        items: [
            {
                name: "Static & Auto",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Static indexes per database",
                community: { value: Infinity },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfStaticIndexesPerDatabase",
            },
            {
                name: "Static indexes per cluster",
                community: { value: Infinity },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfStaticIndexesPerCluster",
            },
            {
                name: "Auto indexes per database",
                community: { value: Infinity },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfAutoIndexesPerDatabase",
            },
            {
                name: "Auto indexes per cluster",
                community: { value: Infinity },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfAutoIndexesPerCluster",
            },
            {
                name: "Corax",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "JavaScript Indexes",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Counters Indexing",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Compare Exchange indexing",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Additional Assemblies from NuGet",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasAdditionalAssembliesFromNuGet",
            },
            {
                name: "Attachments Support",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Rolling Index Deployment",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Index Cleanup",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasIndexCleanup",
            },
        ],
    },
    {
        name: "Sharding",
        link: "https://ravendb.net/features/clusters/sharding",
        items: [
            {
                name: "Single-node sharding",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Multi-node sharding",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasMultiNodeSharding",
            },
            {
                name: "Max replication factor",
                community: { value: 1 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: 1 },
                fieldInLicense: "MaxReplicationFactorForSharding",
            },
        ],
    },
    {
        name: "Time Series",
        link: "https://ravendb.net/features#timeseries",
        items: [
            {
                name: "Time Series",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Incremental Time Series",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Rollups & Retention",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasTimeSeriesRollupsAndRetention",
            },
        ],
    },
    {
        name: "Monitoring",
        link: "https://ravendb.net/features#monitoring",
        items: [
            {
                name: "Cluster Dashboard",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "SNMP",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasSnmpMonitoring",
            },
            {
                name: "Monitoring Endpoints",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasMonitoringEndpoints",
            },
            {
                name: "OpenTelemetry",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasMonitoringEndpoints",
            },
        ],
    },
    {
        name: "Extensions",
        link: "https://ravendb.net/features#extensions",
        items: [
            {
                name: "Counters",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Counters Bulk Insert",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Document Revisions",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Default revisions configuration",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "CanSetupDefaultRevisionsConfiguration",
            },
            {
                name: "Max number of revisions to keep",
                community: { value: 2 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfRevisionsToKeep",
            },
            {
                name: "Max revisions retention time (days)",
                community: { value: 45 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfRevisionAgeToKeepInDays",
            },
            {
                name: "Document Expiration & Refresh",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Min refresh frequency (hrs)",
                community: { value: 36 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MinPeriodForRefreshInHours",
            },
            {
                name: "Min expiration frequency (hrs)",
                community: { value: 36 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MinPeriodForExpirationInHours",
            },
            {
                name: "Attachments",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Documents Compression",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasDocumentsCompression",
            },
            {
                name: "TCP Compression",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasTcpDataCompression",
            },
        ],
    },
    {
        name: "External Replication",
        link: "https://ravendb.net/features/replication/external-replication",
        items: [
            {
                name: "Immediate",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasExternalReplication",
            },
            {
                name: "Delayed",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasDelayedExternalReplication",
            },
            {
                name: "Filtered",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasPullReplicationAsHub",
            },
            {
                name: "Replication Sink",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasPullReplicationAsSink",
            },
            {
                name: "Replication Hub",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasPullReplicationAsHub",
            },
        ],
    },
    {
        name: "Backups",
        link: "https://ravendb.net/features#backups",
        items: [
            {
                name: "Local",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Cloud & Remote",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasCloudBackups",
            },
            {
                name: "Encrypted Backups",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasEncryptedBackups",
            },
            {
                name: "Snapshot Backups",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasSnapshotBackups",
            },
            {
                name: "Periodic Backups",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasPeriodicBackup",
            },
        ],
    },
    {
        name: "Integration",
        link: "https://ravendb.net/features#integration",
        items: [
            {
                name: "RavenDB ETL",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasRavenEtl",
            },
            {
                name: "SQL ETL",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasSqlEtl",
            },
            {
                name: "Snowflake ETL",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasSnowflakeEtl",
            },
            {
                name: "OLAP ETL",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasOlapEtl",
            },
            {
                name: "Elasticsearch ETL",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasElasticSearchEtl",
            },
            {
                name: "PostgreSQL Protocol Support",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasPostgreSqlIntegration",
            },
            {
                name: "Power BI",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasPowerBI",
            },
            {
                name: "ETL to Kafka",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasQueueEtl",
            },
            {
                name: "ETL to RabbitMQ",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasQueueEtl",
            },
            {
                name: "ETL to Azure Queue Storage",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasQueueEtl",
            },
            {
                name: "Kafka Sink",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasQueueSink",
            },
            {
                name: "RabbitMQ Sink",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasQueueSink",
            },
            {
                name: "Grafana plugin for data",
                ...availableEverywhere,
                fieldInLicense: null,
            },
        ],
    },
    {
        name: "Security",
        link: "https://ravendb.net/features#security",
        items: [
            {
                name: "Certificates",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Read-Only Certificates",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasReadOnlyCertificates",
            },
            {
                name: "Encryption in transit",
                community: { value: "TLS 1.3 & X.509" },
                professional: { value: "TLS 1.3 & X.509" },
                enterprise: { value: "TLS 1.3 & X.509" },
                developer: { value: "TLS 1.3 & X.509" },
                fieldInLicense: null,
            },
            {
                name: "Storage encryption",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasEncryption",
            },
        ],
    },
    {
        name: "Subscriptions",
        link: "https://ravendb.net/features/extensions/subscriptions",
        items: [
            {
                name: "Data Subscriptions",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Concurrent Data Subscriptions",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasConcurrentDataSubscriptions",
            },
            {
                name: "Revisions in Subscriptions",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasRevisionsInSubscriptions",
            },
            {
                name: "Subscriptions per database",
                community: { value: 3 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfSubscriptionsPerDatabase",
            },
            {
                name: "Subscriptions per cluster",
                community: { value: 15 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfSubscriptionsPerCluster",
            },
        ],
    },
    {
        name: "Server-Wide",
        link: "https://ravendb.net/features/administration/periodic-backups",
        items: [
            {
                name: "Backups",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasServerWideBackups",
            },
            {
                name: "External Replications",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasServerWideExternalReplications",
            },
            {
                name: "Custom Sorters",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasServerWideCustomSorters",
            },
            {
                name: "Custom Analyzers",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasServerWideAnalyzers",
            },
        ],
    },
    {
        name: "Settings",
        link: null,
        items: [
            {
                name: "Data Archival",
                community: { value: false },
                professional: { value: false },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasDataArchival",
            },
            {
                name: "Custom Sorters",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Custom Sorters per database",
                community: { value: 1 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfCustomSortersPerDatabase",
            },
            {
                name: "Custom Sorters per cluster",
                community: { value: 5 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfCustomSortersPerCluster",
            },
            {
                name: "Custom Analyzers",
                ...availableEverywhere,
                fieldInLicense: null,
            },
            {
                name: "Custom Analyzers per database",
                community: { value: 1 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfCustomAnalyzersPerDatabase",
            },
            {
                name: "Custom Analyzers per cluster",
                community: { value: 5 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
                developer: { value: Infinity },
                fieldInLicense: "MaxNumberOfCustomAnalyzersPerCluster",
            },
            {
                name: "Client Configuration",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasClientConfiguration",
            },
            {
                name: "Studio Configuration",
                community: { value: false },
                professional: { value: true },
                enterprise: { value: true },
                developer: { value: true },
                fieldInLicense: "HasStudioConfiguration",
            },
        ],
    },
];

type LicenseColumn = "community" | "professional" | "enterprise" | "developer";

function getColumns(license: Raven.Server.Commercial.LicenseType): {
    columns: LicenseColumn[];
    current: LicenseColumn;
} {
    switch (license) {
        case "Developer":
            return {
                columns: ["community", "professional", "enterprise", "developer"],
                current: "developer",
            };
        case "Professional":
            return {
                columns: ["professional", "enterprise"],
                current: "professional",
            };
        case "Enterprise":
            return {
                columns: ["enterprise"],
                current: "enterprise",
            };
        default:
            return {
                columns: ["community", "professional", "enterprise"],
                current: "community",
            };
    }
}

function filterFeatureAvailabilitySection(
    features: FeatureAvailabilitySection[],
    searchText: string,
    viewMode: "showDiff" | "showAll",
    columns: LicenseColumn[],
    getEffectiveValue: (feature: FeatureAvailabilityItem, column: LicenseColumn) => any
) {
    const filteredSectionsBySearchText = features
        .map((section) => {
            if (!searchText) {
                return section;
            }

            const searchLower = searchText.toLocaleLowerCase();

            const sectionNameMatch = (section.name ?? "").toLocaleLowerCase().includes(searchLower);
            if (sectionNameMatch) {
                return section;
            }

            const filteredItems = section.items.filter((x) => x.name.toLocaleLowerCase().includes(searchLower));

            if (filteredItems.length === 0) {
                // skip entire section
                return null;
            }

            return {
                ...section,
                items: filteredItems,
            };
        })
        .filter((x) => x);

    if (viewMode === "showAll") {
        return filteredSectionsBySearchText;
    }

    // filter by second condition: 'view mode'
    return filteredSectionsBySearchText
        .map((section) => {
            const filteredItems = section.items.filter((item) => {
                const values = columns.map((x) => getEffectiveValue(item, x));
                // show if has differences
                return !values.every((x) => x === values[0]);
            });
            if (filteredItems.length === 0) {
                // skip entire section
                return null;
            }

            return {
                ...section,
                items: filteredItems,
            };
        })
        .filter((x) => x);
}

export const forTesting = {
    filterFeatureAvailabilitySection,
    featureAvailabilityData,
};

function FeatureValue(props: { value: AvailabilityValue; suffix: string }) {
    const { value, suffix } = props;
    switch (value) {
        case true:
            return <Icon icon="check" color="success" margin="m-0" />;
        case false:
            return <Icon icon="close" color="secondary" margin="m-0" />;
        case Infinity:
            return <Icon icon="infinity" margin="m-0" />;
        default:
            return value + " " + (suffix ?? "");
    }
}

interface FeatureAvailabilitySection {
    name: string;
    link: string;

    items: FeatureAvailabilityItem[];
}

type DisplayableLicenseField = keyof Omit<LicenseStatus, "Attributes">;

interface FeatureAvailabilityItem {
    name: string;

    fieldInLicense: DisplayableLicenseField;
    community: ValueData;
    professional: ValueData;
    enterprise: ValueData;
    developer: ValueData;
    suffix?: string;
}

type AvailabilityValue = boolean | number | string;

interface ValueData {
    value: AvailabilityValue;
    overwrittenValue?: AvailabilityValue;
}
