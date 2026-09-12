import React, { useState } from "react";
import "./GatherDebugInfo.scss";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import Form from "react-bootstrap/Form";
import { useForm, useWatch } from "react-hook-form";
import { GatherDebugInfoFormData, gatherDebugInfoYupResolver } from "./GatherDebugInfoValidation";
import { FormSwitch } from "components/common/Form";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useGatherDebugInfoHelpers } from "./useGatherDebugInfoHelpers";
import GatherDebugInfoAbortConfirm from "./GatherDebugInfoAbortConfirm";
import GatherDebugInfoDatabaseList from "./GatherDebugInfoDatabaseList";
import { Icon } from "components/common/Icon";
import { AboutViewHeading } from "components/common/AboutView";
import appUrl = require("common/appUrl");

function GatherDebugInfo() {
    const { defaultValues, isDownloading, allDatabaseNames, onClusterDownload, onServerDownload, abortData } =
        useGatherDebugInfoHelpers();

    const { handleSubmit, control, setValue } = useForm<GatherDebugInfoFormData>({
        resolver: gatherDebugInfoYupResolver,
        mode: "all",
        defaultValues,
    });

    const { includeDatabases, isSelectAllDatabases, selectedDatabases } = useWatch({ control });

    const [dbSearchQuery, setDbSearchQuery] = useState("");

    const filteredDatabaseNames = allDatabaseNames.filter((db) =>
        db.toLowerCase().includes(dbSearchQuery.toLowerCase())
    );

    const allFilteredSelected =
        filteredDatabaseNames.length > 0 && filteredDatabaseNames.every((db) => (selectedDatabases ?? []).includes(db));

    const handleToggleDatabase = (dbName: string) => {
        const current = selectedDatabases ?? [];
        const updated = current.includes(dbName) ? current.filter((d) => d !== dbName) : [...current, dbName];
        setValue("selectedDatabases", updated, { shouldValidate: true });
    };

    const handleSelectAllToggle = () => {
        if (allFilteredSelected) {
            const current = selectedDatabases ?? [];
            setValue(
                "selectedDatabases",
                current.filter((d) => !filteredDatabaseNames.includes(d)),
                { shouldValidate: true }
            );
        } else {
            const current = selectedDatabases ?? [];
            const toAdd = filteredDatabaseNames.filter((d) => !current.includes(d));
            setValue("selectedDatabases", [...current, ...toAdd], { shouldValidate: true });
        }
    };

    const handleClusterDownload = handleSubmit(onClusterDownload);
    const handleServerDownload = handleSubmit(onServerDownload);

    return (
        <div className="flex-window padding-xs">
            <div className="bs5 gather-debug-info content-margin">
                <AboutViewHeading title="Gather Debug Info" marginBottom={2} icon="gather-debug-information" />
                <p className="text-muted mb-4">
                    Generate a comprehensive diagnostic package to assist in troubleshooting and resolving issues.
                </p>

                <div className="d-flex justify-content-between align-items-center mb-4 flex-wrap gap-2">
                    <Dropdown as={ButtonGroup}>
                        <ButtonWithSpinner
                            type="button"
                            variant="primary"
                            className="rounded-start-pill"
                            icon="download"
                            isSpinning={isDownloading}
                            onClick={handleClusterDownload}
                        >
                            Download package for entire cluster
                        </ButtonWithSpinner>
                        <Dropdown.Toggle
                            split
                            variant="primary"
                            className="rounded-end-pill"
                            disabled={isDownloading}
                        />
                        <Dropdown.Menu>
                            <Dropdown.Item onClick={handleServerDownload}>
                                <Icon icon="server" />
                                Download for current server only
                            </Dropdown.Item>
                        </Dropdown.Menu>
                    </Dropdown>

                    {isDownloading && (
                        <ButtonWithSpinner
                            className="rounded-pill"
                            icon="cancel"
                            variant="warning"
                            isSpinning={abortData.isAborting}
                            onClick={abortData.toggleIsConfirmVisible}
                        >
                            Abort
                        </ButtonWithSpinner>
                    )}

                    <Button
                        variant="secondary"
                        className="rounded-pill ms-auto"
                        as="a"
                        href={appUrl.forDebugPackageAnalyzer()}
                    >
                        <Icon icon="search" />
                        Debug Package Analyzer
                    </Button>
                </div>

                <div className="d-flex justify-content-between align-items-center mb-2">
                    <h4 className="m-0">Select data source</h4>
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0"
                        onClick={() => {
                            setValue("includeServer", true);
                            setValue("includeDatabases", true);
                            setValue("includeLogs", true);
                        }}
                    >
                        Select all
                    </Button>
                </div>
                <div className="panel-bg-1 p-3 border border-color-light border-radius-xs mb-4">
                    <div className="d-flex justify-content-between align-items-center">
                        <span>Include Server</span>
                        <FormSwitch name="includeServer" control={control} color="primary" />
                    </div>
                    <hr className="my-1" />
                    <div className="d-flex justify-content-between align-items-center">
                        <span>Include Databases</span>
                        <FormSwitch name="includeDatabases" control={control} color="primary" />
                    </div>
                    <hr className="my-1" />
                    <div className="d-flex justify-content-between align-items-center">
                        <span>Include Logs</span>
                        <FormSwitch name="includeLogs" control={control} color="primary" />
                    </div>
                </div>

                {includeDatabases && (
                    <>
                        <h4 className="mb-2">Select databases</h4>
                        <div className="panel-bg-1 p-3 border border-color-light border-radius-xs mb-4">
                            <div className="d-flex gap-2">
                                <button
                                    type="button"
                                    className={`gather-debug-info-scope-btn ${isSelectAllDatabases ? "active" : ""}`}
                                    onClick={() => setValue("isSelectAllDatabases", true)}
                                >
                                    <Icon icon="dbgroup" margin="m-0" />
                                    Export all databases
                                </button>
                                <button
                                    type="button"
                                    className={`gather-debug-info-scope-btn ${!isSelectAllDatabases ? "active" : ""}`}
                                    onClick={() => setValue("isSelectAllDatabases", false)}
                                >
                                    <Icon icon="database" margin="m-0" />
                                    Customize export
                                </button>
                            </div>
                            {!isSelectAllDatabases && (
                                <div className="mt-3 d-flex flex-column gap-3">
                                    <div className="clearable-input gather-debug-info-search">
                                        <Icon icon="search" margin="m-0" />
                                        <Form.Control
                                            type="text"
                                            placeholder="Search for database"
                                            value={dbSearchQuery}
                                            onChange={(e) => setDbSearchQuery(e.target.value)}
                                        />
                                        {dbSearchQuery && (
                                            <div className="clear-button">
                                                <Button
                                                    variant="secondary"
                                                    size="sm"
                                                    onClick={() => setDbSearchQuery("")}
                                                >
                                                    <Icon icon="clear" margin="m-0" />
                                                </Button>
                                            </div>
                                        )}
                                    </div>
                                    <div className="gather-debug-info-db-list">
                                        <div className="gather-debug-info-db-header">
                                            <span className="gather-debug-info-db-name-col fw-semibold">
                                                Database name
                                            </span>
                                            <div className="d-flex align-items-center gap-2">
                                                <span>Select all</span>
                                                <Form.Check
                                                    type="switch"
                                                    checked={allFilteredSelected}
                                                    onChange={handleSelectAllToggle}
                                                    id="select-all-databases"
                                                    label=""
                                                    className="m-0"
                                                />
                                            </div>
                                        </div>
                                        <GatherDebugInfoDatabaseList
                                            databaseNames={filteredDatabaseNames}
                                            selectedDatabases={selectedDatabases ?? []}
                                            onToggle={handleToggleDatabase}
                                        />
                                    </div>
                                </div>
                            )}
                        </div>
                    </>
                )}

                <GatherDebugInfoAbortConfirm
                    isOpen={abortData.isConfirmVisible}
                    onConfirm={abortData.onAbort}
                    toggle={abortData.toggleIsConfirmVisible}
                />
                <form className="d-none" target="hidden-form" method="get" id="downloadInfoPackageForm"></form>
            </div>
        </div>
    );
}

export default GatherDebugInfo;
