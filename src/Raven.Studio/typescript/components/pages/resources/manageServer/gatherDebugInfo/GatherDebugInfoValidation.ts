import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup
    .object({
        includeServer: yup.boolean().required(),
        includeDatabases: yup.boolean().required(),
        includeLogs: yup.boolean().required(),
        isSelectAllDatabases: yup.boolean().required(),
        selectedDatabases: yup
            .array()
            .of(yup.string().required())
            .when(["includeDatabases", "isSelectAllDatabases"], {
                is: (includeDatabases: boolean, isSelectAllDatabases: boolean) => {
                    return includeDatabases && !isSelectAllDatabases;
                },
                then: (schema) => schema.min(1, "Required"),
            }),
    })
    .required();

export const gatherDebugInfoYupResolver = yupResolver(schema);
export type GatherDebugInfoFormData = yup.InferType<typeof schema>;
