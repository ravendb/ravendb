import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import {
    importOptionsSchema,
    transformScriptSchema,
} from "components/pages/database/tasks/importData/shared/importOptionsValidation";

export type AuthenticationMethod = "none" | "apiKey" | "windows";

export const serverUrlSchema = yup.string().required().basicUrl();

const serverSchema = yup.object({
    serverUrl: serverUrlSchema,
    isSkipServerCertificateValidation: yup.boolean(),
    databaseName: yup.string().required(),
    isImportRavenFs: yup.boolean(),
    authenticationMethod: yup.mixed<AuthenticationMethod>(),
    userName: yup
        .string()
        .when("$effectiveAuthenticationMethod", { is: "windows", then: (schema) => schema.required() }),
    password: yup
        .string()
        .when("$effectiveAuthenticationMethod", { is: "windows", then: (schema) => schema.required() }),
    domain: yup.string(),
    apiKey: yup.string().when("$effectiveAuthenticationMethod", { is: "apiKey", then: (schema) => schema.required() }),
    isEnableBasicAuthenticationOverUnsecuredHttp: yup.boolean(),
});

export interface MigrateRavenDbValidationContext {
    isRavenFsImport: boolean;
    effectiveAuthenticationMethod: AuthenticationMethod;
}

const processingSchema = transformScriptSchema.when("$isRavenFsImport", {
    is: true,
    then: (schema) => schema.shape({ transformScript: yup.string() }),
});

export const migrateRavenDbSchema = importOptionsSchema.shape({ server: serverSchema, processing: processingSchema });

export type MigrateRavenDbFormData = yup.InferType<typeof migrateRavenDbSchema>;

export const migrateRavenDbYupResolver = yupResolver(migrateRavenDbSchema);
