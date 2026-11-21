import { yupObjectSchema } from "components/utils/yupUtils";
import {
    AmazonDestination,
    AzureAuthType,
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
import { createTranslator } from "common/i18n/i18n";
import { getFtpProtocol, isSupportedFtpUrl } from "./ftpUrlUtils";
import { formDestinationsUtils } from "./formDestinationsUtils";

const t = createTranslator("formDestinations");

const configSchema = yupObjectSchema<BackupConfigurationScript>({
    isOverrideConfig: yup.boolean(),
    arguments: yup.string().nullable(),
    exec: yup
        .string()
        .nullable()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
    timeoutInMs: yup.number().nullable().positive().integer(),
});

const destinationBaseSchema = yupObjectSchema<FormDestinationDataBase>({
    isEnabled: yup.boolean(),
    config: configSchema,
});

type WithoutBase<T extends FormDestinationDataBase> = Omit<T, "isEnabled" | "isOverrideConfig" | "config">;
type WithoutAmazonAndBase<T extends S3Destination | GlacierDestination> = Omit<
    WithoutBase<T>,
    "awsAccessKey" | "awsRegionName" | "awsSecretKey" | "remoteFolderName" | "awsSessionToken"
>;

const isStaticCredentialsEnabled = (isEnabled: boolean, config: BackupConfigurationScript) =>
    isEnabled && !config.isOverrideConfig;

const yupRequiredStringForEnabled = yup
    .string()
    .nullable()
    .when(["isEnabled", "config"], {
        is: isStaticCredentialsEnabled,
        then: (schema) => schema.required(),
    });

const yupRequiredTrimmedStringForEnabled = yup
    .string()
    .trim()
    .nullable()
    .when(["isEnabled", "config"], {
        is: isStaticCredentialsEnabled,
        then: (schema) => schema.required(),
    });

const localSchema = yupObjectSchema<WithoutBase<LocalDestination>>({
    folderPath: yupRequiredTrimmedStringForEnabled,
}).concat(destinationBaseSchema);

const amazonSchema = yupObjectSchema<AmazonDestination>({
    remoteFolderName: yup.string().trim().nullable(),
    awsRegionName: yup
        .string()
        .trim()
        .nullable()
        .when(["isEnabled", "isUseCustomHost", "config"], {
            is: (isEnabled: boolean, isUseCustomHost: boolean, config: BackupConfigurationScript) =>
                isEnabled && !isUseCustomHost && !config.isOverrideConfig,
            then: (schema) =>
                schema.required().test(
                    "include-dash",
                    () => t("validation.awsRegionDash"),
                    (value) => value.includes("-") && !value.startsWith("-") && !value.endsWith("-")
                ),
        }),
    awsAccessKey: yupRequiredTrimmedStringForEnabled,
    awsSecretKey: yupRequiredTrimmedStringForEnabled,
    awsSessionToken: yup.string().trim().nullable(),
});

export const s3Schema = yupObjectSchema<WithoutAmazonAndBase<S3Destination>>({
    bucketName: yup
        .string()
        .trim()
        .nullable()
        .when(["isEnabled", "config"], {
            is: isStaticCredentialsEnabled,
            then: (schema) =>
                schema
                    .required()
                    .min(3)
                    .max(63)
                    .matches(/^[a-z\d]/, () => t("validation.s3BucketStart"))
                    .matches(/[a-z\d]$/, () => t("validation.s3BucketEnd"))
                    .matches(/^[a-z0-9.-]+$/, () => t("validation.s3BucketAllowedCharacters"))
                    .test(
                        "dashes-next-to-period",
                        () => t("validation.s3BucketDashesNextToPeriods"),
                        (value) => !value.includes(".-") && !value.includes("-.")
                    )
                    .test(
                        "consecutive-periods",
                        () => t("validation.s3BucketConsecutivePeriods"),
                        (value) => !value.includes("..")
                    )
                    .test(
                        "ip",
                        () => t("validation.s3BucketIpAddress"),
                        (value) => !/^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/.test(value)
                    ),
        }),
    forcePathStyle: yup.boolean(),
    disableChecksumValidation: yup.boolean(),
    storageClass: yup.string<Raven.Client.Documents.Operations.Backups.S3StorageClass>().nullable(),
    isUseCustomHost: yup.boolean(),
    customServerUrl: yup
        .string()
        .trim()
        .nullable()
        .when(["isUseCustomHost", "config"], {
            is: (isUseCustomHost: boolean, config: BackupConfigurationScript) =>
                isUseCustomHost && !config.isOverrideConfig,
            then: (schema) => schema.required().basicUrl(),
        }),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

function yupRequiredStringForAzureAuthType(authType: AzureAuthType) {
    return yup
        .string()
        .trim()
        .nullable()
        .when(["isEnabled", "config", "authType"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript, selectedAuthType: AzureAuthType) =>
                isEnabled && !config.isOverrideConfig && selectedAuthType === authType,
            then: (schema) => schema.required(),
        });
}

export const azureSchema = yupObjectSchema<WithoutBase<AzureDestination>>({
    storageContainer: yup
        .string()
        .trim()
        .nullable()
        .when(["isEnabled", "config"], {
            is: isStaticCredentialsEnabled,
            then: (schema) =>
                schema
                    .required()
                    .min(3)
                    .max(63)
                    .matches(/^[a-z0-9-]+$/, () => t("validation.azureContainerAllowedCharacters"))
                    .matches(/^[a-z0-9]/, () => t("validation.azureContainerStart"))
                    .matches(/[a-z0-9]$/, () => t("validation.azureContainerEnd"))
                    .test(
                        "consecutive-dashes",
                        () => t("validation.azureContainerConsecutiveDashes"),
                        (value) => !/--/.test(value)
                    ),
        }),
    remoteFolderName: yup.string().trim().nullable(),
    accountName: yupRequiredTrimmedStringForEnabled,
    authType: yup.string<AzureAuthType>(),
    accountKey: yupRequiredStringForAzureAuthType("accountKey"),
    sasToken: yupRequiredStringForAzureAuthType("sasToken"),
}).concat(destinationBaseSchema);

const googleCloudSchema = yupObjectSchema<WithoutBase<GoogleCloudDestination>>({
    bucketName: yup
        .string()
        .trim()
        .nullable()
        .when(["isEnabled", "config"], {
            is: isStaticCredentialsEnabled,
            then: (schema) =>
                schema
                    .required()
                    .min(3)
                    .max(222, () => t("validation.googleBucketLength"))
                    .matches(/^[a-z0-9._-]+$/, () => t("validation.googleBucketAllowedCharacters"))
                    .matches(/^[a-z0-9]/, () => t("validation.googleBucketStart"))
                    .matches(/[a-z0-9]$/, () => t("validation.googleBucketEnd"))
                    .test(
                        "dash-period-underscore",
                        () => t("validation.googleBucketAdjacentCharacters"),
                        (value) => {
                            return (
                                !/\.-/.test(value) &&
                                !/\.\./.test(value) &&
                                !/-\./.test(value) &&
                                !/_\./.test(value) &&
                                !/\._/.test(value)
                            );
                        }
                    ),
        }),

    remoteFolderName: yup.string().trim().nullable(),
    googleCredentialsJson: yup
        .string()
        .nullable()
        .when(["isEnabled", "config"], {
            is: isStaticCredentialsEnabled,
            then: (schema) =>
                schema
                    .required()
                    .matches(/"type"/, () => t("validation.googleCredentialsMissingField", { field: "type" }))
                    .matches(/"private_key"/, () =>
                        t("validation.googleCredentialsMissingField", { field: "private_key" })
                    )
                    .matches(/"client_email"/, () =>
                        t("validation.googleCredentialsMissingField", { field: "client_email" })
                    ),
        }),
}).concat(destinationBaseSchema);

const glacierSchema = yupObjectSchema<WithoutAmazonAndBase<GlacierDestination>>({
    vaultName: yup
        .string()
        .trim()
        .nullable()
        .when(["isEnabled", "config"], {
            is: isStaticCredentialsEnabled,
            then: (schema) =>
                schema
                    .required()
                    .min(1)
                    .max(255)
                    .matches(/^[A-Za-z0-9_.-]+$/, () => t("validation.glacierVaultAllowedCharacters")),
        }),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

const ftpSchema = yupObjectSchema<WithoutBase<FtpDestination>>({
    url: yup
        .string()
        .trim()
        .nullable()
        .when(["isEnabled", "config"], {
            is: isStaticCredentialsEnabled,
            then: (schema) =>
                schema.required().test(
                    "ftp-url",
                    () => t("validation.ftpUrl"),
                    (value) => isSupportedFtpUrl(value)
                ),
        }),
    userName: yupRequiredTrimmedStringForEnabled,
    password: yupRequiredStringForEnabled,
    certificateAsBase64: yup
        .string()
        .nullable()
        .when(["isEnabled", "config", "url"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript, url: string) =>
                isEnabled && !config.isOverrideConfig && getFtpProtocol(url) === "ftps",
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
    }).test("at-least-one-destination", (value, ctx) => {
        if (formDestinationsUtils.getEnabledDestinationTypes(value).length > 0) {
            return true;
        }

        return ctx.createError({
            message:
                formDestinationsUtils.getAddedDestinationTypes(value).length > 0
                    ? t("validation.atLeastOneEnabledDestination")
                    : t("validation.atLeastOneDestination"),
        });
    }),
});
