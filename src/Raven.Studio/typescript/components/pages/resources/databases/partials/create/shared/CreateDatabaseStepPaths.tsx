import { PathsConfigurations } from "./createDatabaseSharedValidation";
import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Alert, InputGroup, InputGroupText, Spinner } from "reactstrap";
import { FormCheckbox, FormSelectCreatable } from "components/common/Form";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useServices } from "components/hooks/useServices";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { UseAsyncReturn, useAsync } from "react-async-hook";
import { InputActionMeta } from "react-select";
import { InputNotHidden, SelectOption } from "components/common/select/Select";

interface CreateDatabaseStepPathsProps {
    manualSelectedNodes: string[];
    isBackupFolder: boolean;
    databaseName: string;
}

export default function CreateDatabaseStepPaths({
    databaseName,
    manualSelectedNodes,
    isBackupFolder,
}: CreateDatabaseStepPathsProps) {
    const { control, setValue } = useFormContext<PathsConfigurations>();
    const formValues = useWatch({ control });
    const { resourcesService } = useServices();

    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);
    const selectedNodeTags = manualSelectedNodes ?? allNodeTags;

    // TODO debounce
    const asyncGetLocalFolderPathOptions = useAsync(
        () =>
            resourcesService.getLocalFolderPathOptions(
                formValues.path,
                isBackupFolder,
                activeDatabaseTracker.default.database()
            ),
        [formValues.path]
    );

    // TODO debounce
    const asyncGetDatabaseLocation = useAsync(() => {
        const path = formValues.isPathDefault || !formValues.path ? "" : formValues.path;

        return resourcesService.getDatabaseLocation(databaseName, path);
    }, [formValues.isPathDefault, formValues.path]);

    // TODO make autocomplete component?
    const onPathChange = (value: string, action: InputActionMeta) => {
        if (action?.action !== "input-blur" && action?.action !== "menu-close") {
            setValue("path", value);
        }
    };

    const pathOptions = getAvailableFolderOptions(asyncGetLocalFolderPathOptions.result?.List);

    return (
        <div>
            <h2 className="text-center">Path Configuration</h2>
            <InputGroup className="my-4">
                <InputGroupText>
                    <FormCheckbox control={control} name="isPathDefault">
                        Use server directory
                    </FormCheckbox>
                </InputGroupText>
                <FormSelectCreatable
                    control={control}
                    name="path"
                    placeholder={formValues.isPathDefault ? "" : "Enter database directory"}
                    options={pathOptions}
                    inputValue={formValues.path ?? ""}
                    onInputChange={onPathChange}
                    components={{ Input: InputNotHidden }}
                    tabSelectsValue
                    blurInputOnSelect={false}
                    isDisabled={formValues.isPathDefault}
                />
            </InputGroup>
            <PathInfo asyncGetDatabaseLocation={asyncGetDatabaseLocation} nodeTagsToDisplay={selectedNodeTags} />
        </div>
    );
}

function getAvailableFolderOptions(backupLocation: string[]): SelectOption[] {
    if (!backupLocation) {
        return [];
    }

    return backupLocation.map((x) => ({ value: x, label: x }));
}

function PathInfo({
    asyncGetDatabaseLocation,
    nodeTagsToDisplay,
}: {
    asyncGetDatabaseLocation: UseAsyncReturn<Raven.Server.Web.Studio.DataDirectoryResult, (string | boolean)[]>;
    nodeTagsToDisplay?: string[];
}) {
    if (asyncGetDatabaseLocation.status === "not-requested" || asyncGetDatabaseLocation.status === "error") {
        return null;
    }

    if (asyncGetDatabaseLocation.status === "loading") {
        return (
            <Alert color="info" className="mt-2">
                <Spinner />
            </Alert>
        );
    }

    if (asyncGetDatabaseLocation.status === "success" && asyncGetDatabaseLocation.result?.List?.length > 0) {
        const filteredLocations = nodeTagsToDisplay
            ? asyncGetDatabaseLocation.result.List.filter((x) => nodeTagsToDisplay.includes(x.NodeTag))
            : asyncGetDatabaseLocation.result.List;

        return (
            <Alert color="info" className="mt-2">
                {filteredLocations.map((location) => (
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
