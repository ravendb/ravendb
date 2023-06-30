import React from "react";
import { Button, Card, CardBody, Col, Form, InputGroup } from "reactstrap";
import "./GatherDebugInfo.scss";
import { SubmitHandler, useForm } from "react-hook-form";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { GatherDebugInfoFormData, gatherDebugInfoYupResolver } from "./GatherDebugInfoValidation";
import { FormCheckboxOption, FormCheckboxes, FormSelect, FormSwitch } from "components/common/Form";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import assertUnreachable from "components/utils/assertUnreachable";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { tryHandleSubmit } from "components/utils/common";
import { DevTool } from "@hookform/devtools";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import messagePublisher from "common/messagePublisher";
import { useServices } from "components/hooks/useServices";
import notificationCenter from "common/notifications/notificationCenter";
import appUrl from "common/appUrl";
import downloader from "common/downloader";
import { getInitialValues, IconList, dataTypesOptions, packageScopeOptions } from "./GatherDebugInfoHelpers";
import endpoints from "endpoints";
import useBoolean from "components/hooks/useBoolean";
import { Icon } from "components/common/Icon";

const adminDebugInfoPackage = endpoints.global.serverWideDebugInfoPackage.adminDebugInfoPackage;
const adminDebugClusterInfoPackage = endpoints.global.serverWideDebugInfoPackage.adminDebugClusterInfoPackage;

const infoPackageImg = require("Content/img/info_package.svg");
const createPackageImg = require("Content/img/create_package.svg");

interface DownloadPackageRequestDto {
    operationId: number;
    type: string;
    database?: string[];
}

function GatherDebugInfo() {
    const { value: inProgress, setValue: setInProgress } = useBoolean(false);
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabases).map((x) => x.name);

    const { watch, handleSubmit, control, formState } = useForm<GatherDebugInfoFormData>({
        resolver: gatherDebugInfoYupResolver,
        mode: "all",
        defaultValues: getInitialValues(allDatabaseNames),
    });

    useDirtyFlag(formState.isDirty);

    const { reportEvent } = useEventsCollector();
    const { databasesService } = useServices();

    // TODO kalczur - allow to customize text for dirty flag modal

    const getNextOperationId = async () => {
        try {
            return await databasesService.getNextOperationId(null);
        } catch (e) {
            messagePublisher.reportError("Could not get next task id.", e.responseText, e.statusText);
            throw e;
        }
    };

    const startDownload = async (formData: GatherDebugInfoFormData, url: string) => {
        setInProgress(true);
        const operationId = await getNextOperationId();

        const urlParams: DownloadPackageRequestDto = {
            operationId,
            type: formData.dataTypes.join(","),
            database: formData.isSelectAllDatabases ? undefined : formData.selectedDatabases,
        };

        const $form = $("#downloadInfoPackageForm");
        $form.attr("action", appUrl.baseUrl + url);
        downloader.fillHiddenFields(urlParams, $form);
        $form.submit();
        console.log("kalczur koniec");

        notificationCenter.instance.monitorOperation(null, operationId).always(() => {
            setInProgress(false);
        });
    };

    const onSave: SubmitHandler<GatherDebugInfoFormData> = async (formData) => {
        tryHandleSubmit(async () => {
            switch (formData.packageScope) {
                case "cluster": {
                    reportEvent("info-package", "cluster-wide");
                    await startDownload(formData, adminDebugClusterInfoPackage);
                    break;
                }
                case "server": {
                    reportEvent("info-package", "server-wide");
                    await startDownload(formData, adminDebugInfoPackage);
                    break;
                }
                default:
                    assertUnreachable(formData.packageScope);
            }
        });
    };

    // TODO on abort

    const databaseOptions: FormCheckboxOption[] = allDatabaseNames.map((x) => ({ value: x, label: x }));

    return (
        <Col lg="6" md="9" sm="12" className="gather-debug-info content-margin">
            <DevTool control={control} />
            <Card>
                <Form onSubmit={handleSubmit(onSave)}>
                    <CardBody className="d-flex flex-center flex-column">
                        <img src={infoPackageImg} alt="Info Package" width="120" />
                        <h3 className="mt-3">Create Debug Package</h3>
                        <p className="lead text-center w-75 fs-5">
                            Generate a comprehensive diagnostic package to assist in troubleshooting and resolving
                            issues.
                        </p>
                        <IconList />
                        <div className="position-relative d-flex flex-row gap-4 w-100 flex-wrap">
                            <div className="d-flex flex-column half-width-section">
                                <h4>Select data source</h4>
                                <div className="d-flex flex-column well px-4 py-3 border-radius-xs">
                                    <FormCheckboxes name="dataTypes" options={dataTypesOptions} control={control} />
                                </div>
                                <h4 className="mt-3 d-flex justify-content-between align-items-center">
                                    Select databases
                                    <FormSwitch
                                        name="isSelectAllDatabases"
                                        color="primary"
                                        control={control}
                                        disabled={allDatabaseNames.length === 0}
                                    >
                                        Select all
                                    </FormSwitch>
                                </h4>
                                {!watch("isSelectAllDatabases") && (
                                    <div className="well px-4 py-3 border-radius-xs">
                                        <FormCheckboxes
                                            name="selectedDatabases"
                                            options={databaseOptions}
                                            control={control}
                                        />
                                    </div>
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
                                            isSpinning={inProgress}
                                        >
                                            Download
                                        </ButtonWithSpinner>
                                        {inProgress && (
                                            <Button>
                                                <Icon icon="cancel" className="rounded-pill me-1" />
                                                Abort
                                            </Button>
                                        )}
                                    </InputGroup>
                                </div>
                            </div>
                        </div>
                    </CardBody>
                </Form>
            </Card>
            <form className="d-none" target="hidden-form" method="get" id="downloadInfoPackageForm"></form>
        </Col>
    );
}

export default GatherDebugInfo;
