import { FormCheckboxOption } from "components/common/Form";
import { SelectOption } from "components/common/Select";
import React from "react";
import IconName from "typings/server/icons";
import {
    GatherDebugInfoFormData,
    allGatherDebugInfoPackageDataTypes,
    GatherDebugInfoPackageScope,
    allGatherDebugInfoPackageScopes,
} from "./GatherDebugInfoValidation";
import assertUnreachable from "components/utils/assertUnreachable";
import DebugInfoPackageContentType = Raven.Server.Documents.Handlers.Debugging.ServerWideDebugInfoPackageHandler.DebugInfoPackageContentType;
import { Icon } from "components/common/Icon";

export function IconList() {
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

export function getInitialValues(allDatabaseNames: string[]): Required<GatherDebugInfoFormData> {
    return {
        dataTypes: allGatherDebugInfoPackageDataTypes,
        isSelectAllDatabases: true,
        selectedDatabases: allDatabaseNames,
        packageScope: null,
    };
}

export const packageScopeOptions: SelectOption<GatherDebugInfoPackageScope>[] = allGatherDebugInfoPackageScopes.map(
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

export const dataTypesOptions: FormCheckboxOption<DebugInfoPackageContentType>[] =
    allGatherDebugInfoPackageDataTypes.map((dataType) => {
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
    });

// TODO
// export function useGatherDebugInfoHelpers() {
//     return {
//         getInitialValues,
//         packageScopeOptions,
//         dataTypesOptions,
//     };
// }
