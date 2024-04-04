import React from "react";
import { Alert, Card, CardBody, Collapse, Label, Spinner } from "reactstrap";
import { FormSelectAutocomplete, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useServices } from "components/hooks/useServices";
import { UseAsyncReturn, useAsync } from "react-async-hook";
import { SelectOption } from "../select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";

export default function Local() {
    const { control } = useFormContext<FormDestinations>();
    const {
        destinations: { local: formValues },
    } = useWatch({ control });

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();

    const asyncGetLocalFolderPathOptions = useAsync(
        () => tasksService.getLocalFolderPathOptions(formValues.folderPath, databaseName),
        [formValues.folderPath]
    );
    const asyncGetBackupLocation = useAsync(() => {
        if (!formValues.folderPath) {
            return;
        }
        return tasksService.getBackupLocation(formValues.folderPath, databaseName);
    }, [formValues.folderPath]);

    const pathOptions = getAvailableFolderOptions(asyncGetLocalFolderPathOptions.result?.List);

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Local
                </FormSwitch>
                <Collapse isOpen={formValues?.isEnabled} className="mt-2">
                    <FormSwitch
                        control={control}
                        name={`${fieldBase}.config.isOverrideConfig`}
                        className="ms-3 mb-2"
                        color="secondary"
                    >
                        Override configuration via external script
                    </FormSwitch>

                    {formValues.config.isOverrideConfig ? (
                        <OverrideConfiguration fieldBase={fieldBase} />
                    ) : (
                        <div className="mt-2">
                            <Label>Folder path</Label>
                            <FormSelectAutocomplete
                                control={control}
                                name={getName("folderPath")}
                                placeholder="Full directory path"
                                options={pathOptions}
                            />
                            <PathInfo
                                asyncGetBackupLocation={asyncGetBackupLocation}
                                hasValue={!!formValues.folderPath}
                            />
                        </div>
                    )}
                </Collapse>
            </CardBody>
        </Card>
    );
}

function PathInfo({
    asyncGetBackupLocation,
    hasValue,
}: {
    asyncGetBackupLocation: UseAsyncReturn<Raven.Server.Web.Studio.DataDirectoryResult, string[]>;
    hasValue: boolean;
}) {
    if (!hasValue || asyncGetBackupLocation.status === "not-requested" || asyncGetBackupLocation.status === "error") {
        return null;
    }

    if (asyncGetBackupLocation.status === "loading") {
        return (
            <Alert color="info" className="mt-2">
                <Spinner />
            </Alert>
        );
    }

    if (asyncGetBackupLocation.status === "success" && asyncGetBackupLocation.result?.List?.length > 0) {
        return (
            <Alert color="info" className="mt-2">
                {asyncGetBackupLocation.result.List.map((location) => (
                    <div key={location.FullPath}>
                        <small>
                            <span>
                                Node tag: <strong>{location.NodeTag}</strong>
                            </span>
                            <br />
                            <span>
                                Path: <strong>{location.FullPath}</strong>
                            </span>
                            <br />
                            {location.Error ? (
                                <strong>{location.Error}</strong>
                            ) : (
                                <>
                                    {location.FreeSpaceHumane ? (
                                        <span>
                                            Free space: <strong>{location.FreeSpaceHumane}</strong>{" "}
                                            {location.TotalSpaceHumane && (
                                                <span>
                                                    {"(Total: "}
                                                    <strong>{location.TotalSpaceHumane}</strong>
                                                    {")"}
                                                </span>
                                            )}
                                        </span>
                                    ) : (
                                        <strong>(Path is unreachable)</strong>
                                    )}
                                </>
                            )}
                        </small>
                    </div>
                ))}
            </Alert>
        );
    }
}

const fieldBase = "destinations.local";

type FormFieldNames = keyof FormDestinations["destinations"]["local"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}

function getAvailableFolderOptions(backupLocation: string[]): SelectOption[] {
    if (!backupLocation) {
        return [];
    }

    return backupLocation.map((x) => ({ value: x, label: x }));
}
