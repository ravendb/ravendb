import { Icon } from "components/common/Icon";
import React from "react";
import { Card, CardBody, Col, Form, InputGroup } from "reactstrap";
import IconName from "typings/server/icons";
import "./GatherDebugInfo.scss";
import { SubmitHandler, useForm } from "react-hook-form";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import {
    GatherDebugInfoFormData,
    gatherDebugInfoYupResolver,
    GatherDebugInfoPackageScope,
    allGatherDebugInfoPackageScopes,
} from "./GatherDebugInfoValidation";
import { FormCheckbox, FormCheckboxOption, FormCheckboxes, FormSelect, FormSwitch } from "components/common/Form";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { SelectOption } from "components/common/Select";
import assertUnreachable from "components/utils/assertUnreachable";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { tryHandleSubmit } from "components/utils/common";
import { DevTool } from "@hookform/devtools";

const infoPackageImg = require("Content/img/info_package.svg");
const createPackageImg = require("Content/img/create_package.svg");

function GatherDebugInfo() {
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabases).map((x) => x.name);

    const { watch, handleSubmit, control, reset, formState } = useForm<GatherDebugInfoFormData>({
        resolver: gatherDebugInfoYupResolver,
        mode: "all",
        defaultValues: {
            isSourceDatabases: true,
            isSourceLogs: true,
            isSourceServer: true,
            isSelectAllDatabases: true,
            selectedDatabases: allDatabaseNames,
            packageScope: null,
        },
    });

    useDirtyFlag(formState.isDirty);

    // TODO kalczur - allow to customize text for dirty flag modal

    const onSave: SubmitHandler<GatherDebugInfoFormData> = async (formData) => {
        tryHandleSubmit(async () => {
            // if (formData.)
            // TODO kalczur

            reset(formData);
        });
    };

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
                                    <FormCheckbox name="isSourceServer" control={control}>
                                        Server
                                    </FormCheckbox>
                                    <FormCheckbox name="isSourceDatabases" control={control}>
                                        Databases
                                    </FormCheckbox>
                                    <FormCheckbox name="isSourceLogs" control={control}>
                                        Logs
                                    </FormCheckbox>
                                </div>
                                <h4 className="mt-3 d-flex justify-content-between align-items-center">
                                    Select databases
                                    <FormSwitch name="isSelectAllDatabases" color="primary" control={control}>
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
                                            disabled={!formState.isDirty}
                                            icon="default"
                                            isSpinning={false}
                                        >
                                            Download
                                        </ButtonWithSpinner>
                                    </InputGroup>
                                </div>
                            </div>
                        </div>
                    </CardBody>
                </Form>
            </Card>
        </Col>
    );
}

function IconList() {
    const icons: IconName[] = ["replication", "stats", "io-test", "storage", "memory", "other"];
    const labels = ["Replication", "Performance", "I/O", "Storage", "Memory", "Other"];
    return (
        <div className="d-flex flex-row my-3 gap-4 flex-wrap justify-content-center icons-list">
            {icons.map((icon, index) => (
                <div key={icon} className="d-flex flex-column align-items-center text-center gap-3">
                    <Icon icon={icon} margin="m-0" />
                    <p>{labels[index]}</p>
                </div>
            ))}
        </div>
    );
}

const packageScopeOptions: SelectOption<GatherDebugInfoPackageScope>[] = allGatherDebugInfoPackageScopes.map(
    (scope) => {
        switch (scope) {
            case "cluster":
                return { label: "Entire cluster", value: scope };
            case "server":
                return { label: "Current server only", value: scope };
            default:
                assertUnreachable(scope);
        }
    }
);

export default GatherDebugInfo;
