import React, { ReactNode } from "react";
import { Control, FieldPath, useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import LicenseRestrictedBadge, { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import {
    connectionStringKeys,
    databaseSettingKeys,
    ImportFromFileFormData,
    ongoingTaskKeys,
} from "../importFromFileValidation";
import { useImportLicenseRestrictions } from "../useImportLicenseRestrictions";
import Card from "react-bootstrap/Card";
import {
    connectionStringLabels,
    databaseSettingLabels,
    ongoingTaskLabels,
} from "components/pages/database/tasks/importData/importFromFile/sections/configurationToImportSectionUtils";
import classNames from "classnames";

export default function ConfigurationToImportSection() {
    const { control, setValue, resetField } = useFormContext<ImportFromFileFormData>();
    const {
        isSettingRestricted,
        getRestrictionTooltip,
        getLicenseRequired,
        isOngoingTaskRestricted,
        getOngoingTaskRestrictionTooltip,
        getOngoingTaskLicenseRequired,
    } = useImportLicenseRestrictions();

    const isIncludeTasks = useWatch({ control, name: "configuration.isIncludeConnectionStringsAndOngoingTasks" });
    const isCustomizeTasks = useWatch({ control, name: "configuration.isCustomizeOngoingTasks" });
    const isImportAllSettings = useWatch({ control, name: "configuration.isImportAllSettings" });
    const databaseSettings = useWatch({ control, name: "configuration.databaseSettings" });
    const isIncludeIndexes = useWatch({ control, name: "configuration.isIncludeIndexes" });
    const isIncludeIndexHistory = useWatch({ control, name: "configuration.isIncludeIndexHistory" });
    const isIncludeIdentities = useWatch({ control, name: "configuration.isIncludeIdentities" });

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

    const areAllEntitiesSelected = isIncludeIndexes && isIncludeIndexHistory && isIncludeIdentities && isIncludeTasks;

    const ongoingTasks = useWatch({ control, name: "configuration.ongoingTasks" });
    const connectionStrings = useWatch({ control, name: "configuration.connectionStrings" });

    const selectableOngoingTaskKeys = ongoingTaskKeys.filter((key) => !isOngoingTaskRestricted(key));

    const areAllOngoingTasksSelected = selectableOngoingTaskKeys.every((key) => ongoingTasks[key]);
    const areAllConnectionStringsSelected = connectionStringKeys.every((key) => connectionStrings[key]);

    const setAllOngoingTasks = (value: boolean) => {
        selectableOngoingTaskKeys.forEach((key) =>
            setValue(`configuration.ongoingTasks.${key}`, value, { shouldDirty: true })
        );
    };

    const setAllConnectionStrings = (value: boolean) => {
        connectionStringKeys.forEach((key) =>
            setValue(`configuration.connectionStrings.${key}`, value, { shouldDirty: true })
        );
    };

    const resetCustomizedTasksToDefault = () => {
        ongoingTaskKeys.forEach((key) => resetField(`configuration.ongoingTasks.${key}`));
        connectionStringKeys.forEach((key) => resetField(`configuration.connectionStrings.${key}`));
        setValue("configuration.isCustomizeOngoingTasks", false, { shouldDirty: true });
    };

    const setAllEntities = (value: boolean) => {
        setValue("configuration.isIncludeIndexes", value, { shouldDirty: true });
        setValue("configuration.isIncludeIndexHistory", value, { shouldDirty: true });
        setValue("configuration.isIncludeIdentities", value, { shouldDirty: true });
        setValue("configuration.isIncludeConnectionStringsAndOngoingTasks", value, { shouldDirty: true });
    };

    return (
        <ImportSection id="configuration-to-import" title="Configuration to import">
            <div className="d-flex justify-content-between align-items-center mb-2">
                <div id="database-entities" className="small-label">
                    Select database entities
                </div>
                <Button variant="link" size="sm" onClick={() => setAllEntities(!areAllEntitiesSelected)}>
                    {areAllEntitiesSelected ? "Deselect all" : "Select all"}
                </Button>
            </div>
            <div className="card p-4 mb-4">
                <FormSwitch control={control} name="configuration.isIncludeIndexes" className="pb-1">
                    Include Indexes
                </FormSwitch>
                <div className="ms-4 d-flex flex-column gap-1">
                    <FormSwitch
                        control={control}
                        name="configuration.isIncludeIndexHistory"
                        afterChange={forceIndexesOn}
                    >
                        Include Index History
                    </FormSwitch>
                    <FormSwitch control={control} name="configuration.isRemoveAnalyzers" afterChange={forceIndexesOn}>
                        Remove Analyzers
                    </FormSwitch>
                </div>
                <hr className="my-1" />
                <FormSwitch control={control} name="configuration.isIncludeIdentities">
                    Include Identities
                </FormSwitch>
                <hr className="my-1" />
                <div className="d-flex justify-content-between align-items-start">
                    <FormSwitch control={control} name="configuration.isIncludeConnectionStringsAndOngoingTasks">
                        Include Connection Strings &amp; Ongoing Tasks
                    </FormSwitch>
                    <Button
                        variant="link"
                        size="sm"
                        disabled={!isIncludeTasks}
                        onClick={() =>
                            isCustomizeTasks
                                ? resetCustomizedTasksToDefault()
                                : setValue("configuration.isCustomizeOngoingTasks", true, { shouldDirty: true })
                        }
                    >
                        {isCustomizeTasks ? "Reset to default" : "Customize"}
                    </Button>
                </div>
                <Collapse in={isIncludeTasks && isCustomizeTasks}>
                    <div>
                        <div className="row mt-3">
                            <div className="col-md-6">
                                <div className="import-list-header mb-2">
                                    <span className="flex-grow-1 fw-semibold">Ongoing tasks</span>
                                    <div className="d-flex align-items-center gap-2">
                                        <span>Select all</span>
                                        <Form.Check
                                            type="switch"
                                            id="select-all-ongoing-tasks"
                                            label=""
                                            className="m-0"
                                            checked={areAllOngoingTasksSelected}
                                            onChange={(e) => setAllOngoingTasks(e.target.checked)}
                                        />
                                    </div>
                                </div>
                                <div className="d-flex flex-column gap-1">
                                    {ongoingTaskKeys.map((key) => (
                                        <LicenseRestrictedSwitch
                                            key={key}
                                            control={control}
                                            name={`configuration.ongoingTasks.${key}`}
                                            restricted={isOngoingTaskRestricted(key)}
                                            tooltip={getOngoingTaskRestrictionTooltip(key)}
                                            licenseRequired={getOngoingTaskLicenseRequired(key)}
                                        >
                                            {ongoingTaskLabels[key]}
                                        </LicenseRestrictedSwitch>
                                    ))}
                                </div>
                            </div>
                            <div className="col-md-6">
                                <div className="import-list-header mb-2">
                                    <span className="flex-grow-1 fw-semibold">Connection strings</span>
                                    <div className="d-flex align-items-center gap-2">
                                        <span>Select all</span>
                                        <Form.Check
                                            type="switch"
                                            id="select-all-connection-strings"
                                            label=""
                                            className="m-0"
                                            checked={areAllConnectionStringsSelected}
                                            onChange={(e) => setAllConnectionStrings(e.target.checked)}
                                        />
                                    </div>
                                </div>
                                <div className="d-flex flex-column gap-1">
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
            <Card className="p-4">
                <div className="d-flex gap-2">
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: isImportAllSettings })}
                        onClick={() => setValue("configuration.isImportAllSettings", true, { shouldDirty: true })}
                    >
                        <Icon icon="database" margin="m-0" />
                        Import all settings
                    </button>
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: !isImportAllSettings })}
                        onClick={() => setValue("configuration.isImportAllSettings", false, { shouldDirty: true })}
                    >
                        <Icon icon="settings" addon="edit" margin="m-0" />
                        Customize
                    </button>
                </div>
                {!isImportAllSettings && (
                    <div className="mt-4">
                        <div className="import-list-header mb-2">
                            <span className="flex-grow-1 fw-semibold">Setting name</span>
                            <div className="d-flex align-items-center gap-2">
                                <span>Select all</span>
                                <Form.Check
                                    type="switch"
                                    id="select-all-database-settings"
                                    label=""
                                    className="m-0"
                                    checked={areAllSettingsSelected}
                                    onChange={(e) => setAllSettings(e.target.checked)}
                                />
                            </div>
                        </div>
                        <div className="d-flex flex-column gap-1">
                            {databaseSettingKeys.map((key) => (
                                <LicenseRestrictedSwitch
                                    key={key}
                                    control={control}
                                    name={`configuration.databaseSettings.${key}`}
                                    restricted={isSettingRestricted(key)}
                                    tooltip={getRestrictionTooltip(key)}
                                    licenseRequired={getLicenseRequired(key)}
                                >
                                    {databaseSettingLabels[key]}
                                </LicenseRestrictedSwitch>
                            ))}
                        </div>
                    </div>
                )}
            </Card>
        </ImportSection>
    );
}

interface LicenseRestrictedSwitchProps {
    control: Control<ImportFromFileFormData>;
    name: FieldPath<ImportFromFileFormData>;
    restricted: boolean;
    tooltip: string | null;
    licenseRequired: LicenseBadgeText | null;
    children: ReactNode;
}

function LicenseRestrictedSwitch({
    control,
    name,
    restricted,
    tooltip,
    licenseRequired,
    children,
}: LicenseRestrictedSwitchProps) {
    return (
        <div className="d-flex align-items-center gap-2" title={restricted ? tooltip : undefined}>
            <div
                className={classNames({
                    "item-disabled": restricted,
                })}
            >
                <FormSwitch control={control} name={name} {...(restricted && { disabled: true })}>
                    {children}
                </FormSwitch>
            </div>
            {restricted && <LicenseRestrictedBadge licenseRequired={licenseRequired} />}
        </div>
    );
}
