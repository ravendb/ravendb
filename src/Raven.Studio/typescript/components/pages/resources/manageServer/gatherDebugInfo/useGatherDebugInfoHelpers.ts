import { GatherDebugInfoFormData } from "./GatherDebugInfoValidation";
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
import { FormCheckboxesOption } from "components/common/Form";
import DebugInfoPackageContentType = Raven.Server.Documents.Handlers.Debugging.ServerWideDebugInfoPackageHandler.DebugInfoPackageContentType;

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

    useDirtyFlag(isDownloading, confirmLeavingPage);

    const asyncGetNextOperationId = useAsyncCallback(() => databasesService.getNextOperationId(null), {
        onError(error) {
            messagePublisher.reportError("Could not get next task id.", error.message);
            setIsDownloading(false);
        },
    });

    const asyncKillOperation = useAsyncCallback(() =>
        databasesService.killOperation(null, asyncGetNextOperationId.result!)
    );

    const startDownload = async (formData: GatherDebugInfoFormData, url: string) => {
        setIsDownloading(true);
        const operationId = await asyncGetNextOperationId.execute();

        const dataTypes: DebugInfoPackageContentType[] = [];
        if (formData.includeServer) dataTypes.push("ServerWide");
        if (formData.includeDatabases) dataTypes.push("Databases");
        if (formData.includeLogs) dataTypes.push("LogFile");

        const urlParams: DownloadPackageRequestDto = {
            operationId,
            type: dataTypes.join(","),
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

    const onClusterDownload: SubmitHandler<GatherDebugInfoFormData> = async (formData) => {
        tryHandleSubmit(async () => {
            reportEvent("info-package", "cluster-wide");
            await startDownload(formData, adminDebugClusterInfoPackage);
        });
    };

    const onServerDownload: SubmitHandler<GatherDebugInfoFormData> = async (formData) => {
        tryHandleSubmit(async () => {
            reportEvent("info-package", "server-wide");
            await startDownload(formData, adminDebugInfoPackage);
        });
    };

    const databaseOptions: FormCheckboxesOption[] = allDatabaseNames.map((x) => ({ value: x, label: x }));

    return {
        isDownloading,
        defaultValues,
        allDatabaseNames,
        databaseOptions,
        onClusterDownload,
        onServerDownload,
        abortData: {
            isConfirmVisible: isAbortConfirmVisible,
            toggleIsConfirmVisible: toggleIsAbortConfirmVisible,
            onAbort: asyncKillOperation.execute,
            isAborting: asyncKillOperation.loading,
        },
    };
}

const defaultValues: GatherDebugInfoFormData = {
    includeServer: true,
    includeDatabases: true,
    includeLogs: true,
    isSelectAllDatabases: true,
    selectedDatabases: [],
};

function confirmLeavingPage(): JQueryDeferred<confirmDialogResult> {
    const abortResult = $.Deferred<confirmDialogResult>();

    // TODO Use a react component if the dirty flag is already moved

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
