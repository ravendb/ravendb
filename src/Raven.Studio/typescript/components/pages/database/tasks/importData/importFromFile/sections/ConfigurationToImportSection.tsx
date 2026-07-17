import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Table from "react-bootstrap/Table";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import {
    ImportFromFileFormData,
    databaseSettingKeys,
    DatabaseSettingKey,
    ongoingTaskKeys,
    OngoingTaskKey,
    connectionStringKeys,
    ConnectionStringKey,
} from "../importFromFileValidation";
import { useImportLicenseRestrictions } from "../useImportLicenseRestrictions";

const databaseSettingLabels: Record<DatabaseSettingKey, string> = {
    settings: "Settings",
    conflictSolverConfig: "Conflict Solver Configuration",
    client: "Client Configuration",
    revisions: "Revisions Configuration",
    refresh: "Document Refresh",
    expiration: "Document Expiration",
    documentsCompression: "Documents Compression",
    schemaValidation: "Document Schema",
    dataArchival: "Data Archival",
    timeSeries: "Time Series Configuration",
    sorters: "Custom Sorters",
    analyzers: "Custom Analyzers",
    postgreSqlIntegration: "PostgreSQL Integration",
};

const ongoingTaskLabels: Record<OngoingTaskKey, string> = {
    periodicBackups: "Periodic Backups",
    externalReplications: "External Replications",
    ravenEtls: "RavenDB ETLs",
    sqlEtls: "SQL ETLs",
    snowflakeEtls: "Snowflake ETLs",
    olapEtls: "OLAP ETLs",
    elasticSearchEtls: "Elasticsearch ETLs",
    queueEtls: "Queue ETLs (Kafka, RabbitMQ, Azure Queue Storage)",
    hubReplications: "Replication Hubs",
    sinkReplications: "Replication Sinks",
    embeddingsGeneration: "Embeddings Generation",
    genAi: "GenAI",
    cdcSinks: "CDC Sinks",
    aiAgents: "AI Agents",
    remoteAttachments: "Remote Attachments",
};

const connectionStringLabels: Record<ConnectionStringKey, string> = {
    ravenConnectionStrings: "RavenDB Connection Strings",
    sqlConnectionStrings: "SQL Connection Strings",
    snowflakeConnectionStrings: "Snowflake Connection Strings",
    olapConnectionStrings: "OLAP Connection Strings",
    elasticSearchConnectionStrings: "Elasticsearch Connection Strings",
    queueConnectionStrings: "Queue Connection Strings (Kafka, RabbitMQ, Azure Queue Storage)",
    aiConnectionStrings: "AI Connection Strings",
};

export default function ConfigurationToImportSection() {
    const { control, setValue } = useFormContext<ImportFromFileFormData>();
    const { isSettingRestricted, getRestrictionTooltip, getLicenseRequired } = useImportLicenseRestrictions();

    const isIncludeTasks = useWatch({ control, name: "configuration.isIncludeConnectionStringsAndOngoingTasks" });
    const isCustomizeTasks = useWatch({ control, name: "configuration.isCustomizeOngoingTasks" });
    const isImportAllSettings = useWatch({ control, name: "configuration.isImportAllSettings" });
    const databaseSettings = useWatch({ control, name: "configuration.databaseSettings" });

    const forceIndexesOn = (value: boolean) => {
        if (value) {
            setValue("configuration.isIncludeIndexes", true, { shouldDirty: true });
        }
    };

    const selectableSettingKeys = databaseSettingKeys.filter((key) => !isSettingRestricted(key));
    const areAllSettingsSelected = selectableSettingKeys.every((key) => databaseSettings[key]);

    const setAllSettings = (value: boolean) => {
        selectableSettingKeys.forEach((key) =>
            setValue(`configuration.databaseSettings.${key}`, value, { shouldDirty: true })
        );
    };

    const selectAllEntities = () => {
        setValue("configuration.isIncludeIndexes", true, { shouldDirty: true });
        setValue("configuration.isIncludeIndexHistory", true, { shouldDirty: true });
        setValue("configuration.isIncludeIdentities", true, { shouldDirty: true });
        setValue("configuration.isIncludeConnectionStringsAndOngoingTasks", true, { shouldDirty: true });
    };

    return (
        <ImportSection id="configuration-to-import" title="Configuration to import">
            <div className="d-flex justify-content-between align-items-center mb-2">
                <div id="database-entities" className="small-label">
                    Select database entities
                </div>
                <Button variant="link" size="sm" onClick={selectAllEntities}>
                    Select all
                </Button>
            </div>
            <div className="card p-4 mb-4">
                <FormSwitch control={control} name="configuration.isIncludeIndexes">
                    Include Indexes
                </FormSwitch>
                <div className="ms-4">
                    <FormSwitch control={control} name="configuration.isIncludeIndexHistory" afterChange={forceIndexesOn}>
                        Include Index History
                    </FormSwitch>
                    <FormSwitch control={control} name="configuration.isRemoveAnalyzers" afterChange={forceIndexesOn}>
                        Remove Analyzers
                    </FormSwitch>
                </div>
                <hr />
                <FormSwitch control={control} name="configuration.isIncludeIdentities">
                    Include Identities
                </FormSwitch>
                <hr />
                <div className="d-flex justify-content-between align-items-start">
                    <FormSwitch control={control} name="configuration.isIncludeConnectionStringsAndOngoingTasks">
                        Include Connection Strings &amp; Ongoing Tasks
                    </FormSwitch>
                    <Button
                        variant="link"
                        size="sm"
                        disabled={!isIncludeTasks}
                        onClick={() =>
                            setValue("configuration.isCustomizeOngoingTasks", !isCustomizeTasks, { shouldDirty: true })
                        }
                    >
                        Customize
                    </Button>
                </div>
                <Collapse in={isIncludeTasks && isCustomizeTasks}>
                    <div>
                        <div className="row mt-3">
                            <div className="col-md-6">
                                <div className="small-label mb-2">Ongoing tasks</div>
                                {ongoingTaskKeys.map((key) => (
                                    <FormSwitch key={key} control={control} name={`configuration.ongoingTasks.${key}`}>
                                        {ongoingTaskLabels[key]}
                                    </FormSwitch>
                                ))}
                            </div>
                            <div className="col-md-6">
                                <div className="small-label mb-2">Connection strings</div>
                                {connectionStringKeys.map((key) => (
                                    <FormSwitch
                                        key={key}
                                        control={control}
                                        name={`configuration.connectionStrings.${key}`}
                                    >
                                        {connectionStringLabels[key]}
                                    </FormSwitch>
                                ))}
                            </div>
                        </div>
                    </div>
                </Collapse>
                {isIncludeTasks && (
                    <Alert variant="info" className="mt-3 mb-0">
                        <Icon icon="info" /> Imported ongoing tasks will be disabled by default.
                    </Alert>
                )}
            </div>

            <div id="database-settings" className="small-label mb-2">
                Select database settings
            </div>
            <div className="d-flex gap-3 mb-3">
                <Button
                    variant={isImportAllSettings ? "primary" : "outline-secondary"}
                    className="flex-grow-1 py-3"
                    onClick={() => setValue("configuration.isImportAllSettings", true, { shouldDirty: true })}
                >
                    <Icon icon="database" /> Import all settings
                </Button>
                <Button
                    variant={!isImportAllSettings ? "primary" : "outline-secondary"}
                    className="flex-grow-1 py-3"
                    onClick={() => setValue("configuration.isImportAllSettings", false, { shouldDirty: true })}
                >
                    <Icon icon="settings" /> Customize
                </Button>
            </div>
            {!isImportAllSettings && (
                <Table className="mb-0">
                    <thead>
                        <tr>
                            <th>Setting name</th>
                            <th className="text-end">
                                Select all{" "}
                                <Form.Check
                                    inline
                                    type="switch"
                                    aria-label="Select all settings"
                                    checked={areAllSettingsSelected}
                                    onChange={(e) => setAllSettings(e.target.checked)}
                                />
                            </th>
                        </tr>
                    </thead>
                    <tbody>
                        {databaseSettingKeys.map((key) => {
                            const restricted = isSettingRestricted(key);
                            const licenseRequired = getLicenseRequired(key);
                            return (
                                <tr key={key} title={restricted ? getRestrictionTooltip(key) : undefined}>
                                    <td colSpan={2}>
                                        <div className="d-flex align-items-center gap-2">
                                            {/* dim only the switch - the license badge must stay fully visible */}
                                            <div className={restricted ? "item-disabled" : undefined}>
                                                <FormSwitch
                                                    control={control}
                                                    name={`configuration.databaseSettings.${key}`}
                                                    {...(restricted && { disabled: true })}
                                                >
                                                    {databaseSettingLabels[key]}
                                                </FormSwitch>
                                            </div>
                                            {restricted && (
                                                <LicenseRestrictedBadge licenseRequired={licenseRequired} />
                                            )}
                                        </div>
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </Table>
            )}
        </ImportSection>
    );
}
