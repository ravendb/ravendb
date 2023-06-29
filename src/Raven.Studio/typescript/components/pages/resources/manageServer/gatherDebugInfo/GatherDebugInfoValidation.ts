import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { exhaustiveStringTuple } from "components/utils/common";

export type GatherDebugInfoPackageScope = "cluster" | "server";

export const allGatherDebugInfoPackageScopes = exhaustiveStringTuple<GatherDebugInfoPackageScope>()(
    "cluster",
    "server"
);

const schema = yup
    .object({
        isSourceServer: yup.boolean().required(),
        isSourceDatabases: yup.boolean().required(),
        isSourceLogs: yup.boolean().required(),
        isSelectAllDatabases: yup.boolean().required(),
        selectedDatabases: yup.array().of(yup.string()),
        packageScope: yup
            .mixed<GatherDebugInfoPackageScope>()
            .oneOf(allGatherDebugInfoPackageScopes)
            .nullable()
            .required(),
    })
    .required();

export const gatherDebugInfoYupResolver = yupResolver(schema);
export type GatherDebugInfoFormData = yup.InferType<typeof schema>;
