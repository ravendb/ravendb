import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { exhaustiveStringTuple } from "components/utils/common";
import DebugInfoPackageContentType = Raven.Server.Documents.Handlers.Debugging.ServerWideDebugInfoPackageHandler.DebugInfoPackageContentType;

export type GatherDebugInfoPackageScope = "cluster" | "server";

export const allGatherDebugInfoPackageScopes = exhaustiveStringTuple<GatherDebugInfoPackageScope>()(
    "server",
    "cluster"
);

export const allGatherDebugInfoPackageDataTypes = exhaustiveStringTuple<
    Exclude<DebugInfoPackageContentType, "Default">
>()("Databases", "LogFile", "ServerWide");

const schema = yup
    .object({
        dataTypes: yup.array().of(yup.string<DebugInfoPackageContentType>()).required(),
        isSelectAllDatabases: yup.boolean().required(),
        selectedDatabases: yup
            .array()
            .of(yup.string())
            .when(["dataTypes", "isSelectAllDatabases"], {
                is: (dataTypes: DebugInfoPackageContentType, isSelectAllDatabases: boolean) => {
                    return dataTypes.includes("Databases") && !isSelectAllDatabases;
                },
                then: (schema) => schema.required(),
            }),
        packageScope: yup
            .mixed<GatherDebugInfoPackageScope>()
            .oneOf(allGatherDebugInfoPackageScopes)
            .nullable()
            .required(),
    })
    .required();

export const gatherDebugInfoYupResolver = yupResolver(schema);
export type GatherDebugInfoFormData = yup.InferType<typeof schema>;
