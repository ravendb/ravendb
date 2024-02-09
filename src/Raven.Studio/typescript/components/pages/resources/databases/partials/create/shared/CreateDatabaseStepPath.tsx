import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Alert, InputGroup, InputGroupText, Spinner } from "reactstrap";
import { FormCheckbox, FormSelectCreatable } from "components/common/Form";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useServices } from "components/hooks/useServices";
import { InputActionMeta } from "react-select";
import { InputNotHidden, SelectOption } from "components/common/select/Select";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import { UseAsyncReturn } from "react-async-hook";
import { CreateDatabaseFromBackupFormData } from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupValidation";
import { CreateDatabaseRegularFormData } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularValidation";

interface CreateDatabaseStepPathProps {
    manualSelectedNodes: string[];
    isBackupFolder: boolean;
}

export default function CreateDatabaseStepPath({ manualSelectedNodes, isBackupFolder }: CreateDatabaseStepPathProps) {
    const { control, setValue } = useFormContext<CreateDatabaseRegularFormData | CreateDatabaseFromBackupFormData>();
    const formValues = useWatch({ control });
    const { resourcesService } = useServices();

    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);
    const selectedNodeTags = manualSelectedNodes ?? allNodeTags;

    const asyncGetFolderOptions = useAsyncDebounce(resourcesService.getFolderPathOptions_ServerLocal, [
        formValues.pathsConfigurations.path,
        isBackupFolder,
    ]);

    const asyncGetDatabaseLocation = useAsyncDebounce(resourcesService.getDatabaseLocation, [
        formValues.basicInfo.databaseName,
        formValues.pathsConfigurations.path,
    ]);

    // TODO make autocomplete component?
    const onPathChange = (value: string, action: InputActionMeta) => {
        if (action?.action !== "input-blur" && action?.action !== "menu-close") {
            setValue("pathsConfigurations.path", value);
        }
    };

    const pathOptions = getAvailableFolderOptions(asyncGetFolderOptions.result?.List);

    return (
        <div>
            <h2 className="text-center">Path Configuration</h2>
            <InputGroup className="my-4">
                <InputGroupText>
                    <FormCheckbox control={control} name="pathsConfigurations.isDefault">
                        Use server directory
                    </FormCheckbox>
                </InputGroupText>
                <FormSelectCreatable
                    control={control}
                    name="pathsConfigurations.path"
                    placeholder={formValues.pathsConfigurations.isDefault ? "" : "Enter database directory"}
                    options={pathOptions}
                    inputValue={formValues.pathsConfigurations.path ?? ""}
                    onInputChange={onPathChange}
                    components={{ Input: InputNotHidden }}
                    tabSelectsValue
                    blurInputOnSelect={false}
                    isDisabled={formValues.pathsConfigurations.isDefault}
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
