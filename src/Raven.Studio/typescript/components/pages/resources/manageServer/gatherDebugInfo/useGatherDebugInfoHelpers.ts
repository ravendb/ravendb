import { FormCheckboxOption } from "components/common/Form";
import { SelectOption } from "components/common/Select";
import {
    GatherDebugInfoFormData,
    allGatherDebugInfoPackageDataTypes,
    GatherDebugInfoPackageScope,
    allGatherDebugInfoPackageScopes,
} from "./GatherDebugInfoValidation";
import assertUnreachable from "components/utils/assertUnreachable";
import DebugInfoPackageContentType = Raven.Server.Documents.Handlers.Debugging.ServerWideDebugInfoPackageHandler.DebugInfoPackageContentType;
import messagePublisher = require("common/messagePublisher");
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import appUrl = require("common/appUrl");
import downloader = require("common/downloader");
import notificationCenter = require("common/notifications/notificationCenter");
import useBoolean from "components/hooks/useBoolean";
import { tryHandleSubmit } from "components/utils/common";
import { SubmitHandler } from "react-hook-form";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import endpoints = require("endpoints");
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useAsyncCallback } from "react-async-hook";
import viewHelpers = require("common/helpers/view/viewHelpers");

const adminDebugInfoPackage = endpoints.global.serverWideDebugInfoPackage.adminDebugInfoPackage;
const adminDebugClusterInfoPackage = endpoints.global.serverWideDebugInfoPackage.adminDebugClusterInfoPackage;

interface DownloadPackageRequestDto {
    operationId: number;
    type: string;
    database?: string[];
}

export function useGatherDebugInfoHelpers() {
    const { value: isDownloading, setValue: setIsDownloading } = useBoolean(false);
    const { value: isAbortConfirmVisible, toggle: toggleIsAbortConfirmVisible } = useBoolean(false);
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabases).map((x) => x.name);

    const defaultValues = getDefaultValues(allDatabaseNames);
    const databaseOptions: FormCheckboxOption[] = allDatabaseNames.map((x) => ({ value: x, label: x }));

    useDirtyFlag(isDownloading, confirmLeavingPage);

    const asyncGetNextOperationId = useAsyncCallback(() => databasesService.getNextOperationId(null), {
        onError(error) {
            messagePublisher.reportError("Could not get next task id.", error.message);
            setIsDownloading(false);
        },
    });

    const asyncKillOperation = useAsyncCallback(() =>
        databasesService.killOperation(null, asyncGetNextOperationId.result)
    );

    const startDownload = async (formData: GatherDebugInfoFormData, url: string) => {
        setIsDownloading(true);
        // TODO kalczur test if it work if fails
        const operationId = await asyncGetNextOperationId.execute();

        const urlParams: DownloadPackageRequestDto = {
            operationId,
            type: formData.dataTypes.join(","),
            database: formData.isSelectAllDatabases ? undefined : formData.selectedDatabases,
        };

        const $form = $("#downloadInfoPackageForm");
        $form.attr("action", appUrl.baseUrl + url);
        downloader.fillHiddenFields(urlParams, $form);
        $form.submit();

        notificationCenter.instance.monitorOperation(null, operationId).always(() => {
            setIsDownloading(false);
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

    return {
        isDownloading,
        defaultValues,
        databaseOptions,
        packageScopeOptions,
        dataTypesOptions,
        onSave,
        abortData: {
            isConfirmVisible: isAbortConfirmVisible,
            toggleIsConfirmVisible: toggleIsAbortConfirmVisible,
            onAbort: asyncKillOperation.execute,
            isAborting: asyncKillOperation.loading,
        },
    };
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

const dataTypesOptions: FormCheckboxOption<DebugInfoPackageContentType>[] = allGatherDebugInfoPackageDataTypes.map(
    (dataType) => {
        switch (dataType) {
            case "Databases":
                return { label: "Databases", value: dataType };
            case "ServerWide":
                return { label: "Server", value: dataType };
            case "LogFile":
                return { label: "Logs", value: dataType };
            default:
                assertUnreachable(dataType);
        }
    }
);

function getDefaultValues(allDatabaseNames: string[]): Required<GatherDebugInfoFormData> {
    return {
        dataTypes: allGatherDebugInfoPackageDataTypes,
        isSelectAllDatabases: true,
        selectedDatabases: allDatabaseNames,
        packageScope: null,
    };
}

function confirmLeavingPage(): JQueryDeferred<confirmDialogResult> {
    const abortResult = $.Deferred<confirmDialogResult>();

    const confirmation = viewHelpers.confirmationMessage(
        "Abort Debug Package Creation",
        "Leaving this page will abort package creation.<br>How do you want to proceed?",
        {
            buttons: ["Stay on this page", "Leave and Abort"],
            forceRejectWithResolve: true,
            html: true,
        }
    );

    confirmation.done((result: confirmDialogResult) => abortResult.resolve(result));
    return abortResult;
}
