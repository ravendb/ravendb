import { yupObjectSchema } from "components/utils/yupUtils";
import {
    AmazonDestination,
    AzureDestination,
    BackupConfigurationScript,
    FormDestinationDataBase,
    FormDestinations,
    FtpDestination,
    GlacierDestination,
    GoogleCloudDestination,
    LocalDestination,
    S3Destination,
} from "./formDestinationsTypes";
import * as yup from "yup";

// TODO kalczur - for all isEnabled and !isOverrideConfig
// TODO kalczur - check validation

const configSchema = yupObjectSchema<BackupConfigurationScript>({
    arguments: yup
        .string()
        .nullable()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
    exec: yup
        .string()
        .nullable()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
    timeoutInMs: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

const destinationBaseSchema = yupObjectSchema<FormDestinationDataBase>({
    isEnabled: yup.boolean(),
    isOverrideConfig: yup.boolean(),
    config: configSchema,
});

type WithoutBase<T extends FormDestinationDataBase> = Omit<T, "isEnabled" | "isOverrideConfig" | "config">;
type WithoutAmazonAndBase<T extends S3Destination | GlacierDestination> = Omit<
    WithoutBase<T>,
    "awsAccessKey" | "awsRegionName" | "awsSecretKey" | "remoteFolderName"
>;

const localSchema = yupObjectSchema<WithoutBase<LocalDestination>>({
    folderPath: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
}).concat(destinationBaseSchema);

const amazonSchema = yupObjectSchema<AmazonDestination>({
    remoteFolderName: yup.string().nullable(),
    awsRegionName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    awsAccessKey: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    awsSecretKey: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

const s3Schema = yupObjectSchema<WithoutAmazonAndBase<S3Destination>>({
    bucketName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    forcePathStyle: yup.boolean(),
    customServerUrl: yup.string().nullable(),
    isUseCustomHost: yup.boolean(),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

const azureSchema = yupObjectSchema<WithoutBase<AzureDestination>>({
    storageContainer: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    remoteFolderName: yup.string().nullable(),
    accountName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    accountKey: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
}).concat(destinationBaseSchema);

const googleCloudSchema = yupObjectSchema<WithoutBase<GoogleCloudDestination>>({
    bucketName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    remoteFolderName: yup.string().nullable(),
    googleCredentialsJson: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
}).concat(destinationBaseSchema);

const glacierSchema = yupObjectSchema<WithoutAmazonAndBase<GlacierDestination>>({
    vaultName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

const ftpSchema = yupObjectSchema<WithoutBase<Omit<FtpDestination, "certificateAsBase64">>>({
    url: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    userName: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    password: yup
        .string()
        .nullable()
        .when("isEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
}).concat(destinationBaseSchema);

export const destinationsSchema = yup.object({
    destinations: yupObjectSchema<FormDestinations["destinations"]>({
        local: localSchema,
        s3: s3Schema,
        azure: azureSchema,
        googleCloud: googleCloudSchema,
        glacier: glacierSchema,
        ftp: ftpSchema,
    }).test("at-least-one-destination", "Please select at least one destination", (value) => {
        return (
            value.local.isEnabled ||
            value.s3.isEnabled ||
            value.azure.isEnabled ||
            value.googleCloud.isEnabled ||
            value.glacier.isEnabled ||
            value.ftp.isEnabled
        );
    }),
});
