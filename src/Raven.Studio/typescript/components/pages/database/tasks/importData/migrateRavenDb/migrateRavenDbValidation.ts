import * as yup from "yup";
import { importOptionsSchema } from "components/pages/database/tasks/importData/shared/importOptionsValidation";

const serverSchema = yup.object({
    serverUrl: yup.string().required().url(),
    isSkipServerCertificateValidation: yup.boolean(),
    databaseName: yup.string().required(),
    isImportRavenFs: yup.boolean(),
    authenticationMethod: yup.mixed<"none" | "apiKey" | "windows">(),
    userName: yup.string().when("authenticationMethod", { is: "windows", then: (schema) => schema.required() }),
    password: yup.string().when("authenticationMethod", { is: "windows", then: (schema) => schema.required() }),
    domain: yup.string(),
    apiKey: yup.string().when("authenticationMethod", { is: "apiKey", then: (schema) => schema.required() }),
    isEnableBasicAuthenticationOverUnsecuredHttp: yup.boolean(),
});

export const migrateRavenDbSchema = importOptionsSchema.shape({ server: serverSchema });

export type MigrateRavenDbFormData = yup.InferType<typeof migrateRavenDbSchema>;
