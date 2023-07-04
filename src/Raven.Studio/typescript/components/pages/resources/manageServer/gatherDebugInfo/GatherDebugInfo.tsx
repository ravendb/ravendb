import React from "react";
import { Card, CardBody, Col, Form, InputGroup } from "reactstrap";
import "./GatherDebugInfo.scss";
import { useForm, useWatch } from "react-hook-form";
import { GatherDebugInfoFormData, gatherDebugInfoYupResolver } from "./GatherDebugInfoValidation";
import { FormCheckboxes, FormSelect, FormSwitch } from "components/common/Form";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import GatherDebugInfoIcons from "./GatherDebugInfoIcons";
import { useGatherDebugInfoHelpers } from "./useGatherDebugInfoHelpers";
import GatherDebugInfoAbortConfirm from "./GatherDebugInfoAbortConfirm";

const infoPackageImg = require("Content/img/info_package.svg");
const createPackageImg = require("Content/img/create_package.svg");

function GatherDebugInfo() {
    const { dataTypesOptions, defaultValues, packageScopeOptions, isDownloading, databaseOptions, onSave, abortData } =
        useGatherDebugInfoHelpers();

    const { handleSubmit, control } = useForm<GatherDebugInfoFormData>({
        resolver: gatherDebugInfoYupResolver,
        mode: "all",
        defaultValues,
    });

    const { isSelectAllDatabases, dataTypes } = useWatch({ control });

    return (
        <Col lg="6" md="9" sm="12" className="gather-debug-info content-margin">
            <Card>
                <Form onSubmit={handleSubmit(onSave)}>
                    <CardBody className="d-flex flex-center flex-column">
                        <img src={infoPackageImg} alt="Info Package" width="120" />
                        <h3 className="mt-3">Create Debug Package</h3>
                        <p className="lead text-center w-75 fs-5">
                            Generate a comprehensive diagnostic package to assist in troubleshooting and resolving
                            issues.
                        </p>
                        <GatherDebugInfoIcons />
                        <div className="position-relative d-flex flex-row gap-4 w-100 flex-wrap">
                            <div className="d-flex flex-column half-width-section">
                                <h4>Select data source</h4>
                                <div className="d-flex flex-column well px-4 py-3 border-radius-xs">
                                    <FormCheckboxes name="dataTypes" options={dataTypesOptions} control={control} />
                                </div>
                                {dataTypes.includes("Databases") && (
                                    <>
                                        <h4 className="mt-3 d-flex justify-content-between align-items-center">
                                            Select databases
                                            <FormSwitch
                                                name="isSelectAllDatabases"
                                                color="primary"
                                                control={control}
                                                disabled={databaseOptions.length === 0}
                                            >
                                                Select all
                                            </FormSwitch>
                                        </h4>
                                        {!isSelectAllDatabases && (
                                            <div className="well px-4 py-3 border-radius-xs">
                                                <FormCheckboxes
                                                    name="selectedDatabases"
                                                    options={databaseOptions}
                                                    control={control}
                                                />
                                            </div>
                                        )}
                                    </>
                                )}
                            </div>
                            <div className="d-flex flex-column half-width-section">
                                <div className="position-sticky package-download-section d-flex flex-column align-items-center well border-radius-xs p-4 gap-4">
                                    <img src={createPackageImg} alt="Info Package" width="90" />
                                    <h4 className="m-0">Create package for</h4>
                                    <InputGroup className="d-flex flex-column align-items-center gap-4">
                                        <FormSelect
                                            control={control}
                                            name="packageScope"
                                            options={packageScopeOptions}
                                        />
                                        <ButtonWithSpinner
                                            type="submit"
                                            color="primary"
                                            className="rounded-pill"
                                            icon="default"
                                            isSpinning={isDownloading}
                                        >
                                            Download
                                        </ButtonWithSpinner>
                                        {isDownloading && (
                                            <ButtonWithSpinner
                                                className="rounded-pill"
                                                icon="cancel"
                                                color="warning"
                                                isSpinning={abortData.isAborting}
                                                onClick={abortData.toggleIsConfirmVisible}
                                            >
                                                Abort
                                            </ButtonWithSpinner>
                                        )}
                                    </InputGroup>
                                </div>
                            </div>
                        </div>
                    </CardBody>
                </Form>
            </Card>
            <GatherDebugInfoAbortConfirm
                isOpen={abortData.isConfirmVisible}
                onConfirm={abortData.onAbort}
                toggle={abortData.toggleIsConfirmVisible}
            />
            <form className="d-none" target="hidden-form" method="get" id="downloadInfoPackageForm"></form>
        </Col>
    );
}

export default GatherDebugInfo;
