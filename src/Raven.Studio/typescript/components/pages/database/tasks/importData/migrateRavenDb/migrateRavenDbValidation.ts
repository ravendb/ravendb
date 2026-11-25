import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { importOptionsSchema } from "components/pages/database/tasks/importData/shared/importOptionsValidation";

export const serverUrlSchema = yup.string().required().basicUrl();

const serverSchema = yup.object({
    serverUrl: serverUrlSchema,
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

export const migrateRavenDbYupResolver = yupResolver(migrateRavenDbSchema);
