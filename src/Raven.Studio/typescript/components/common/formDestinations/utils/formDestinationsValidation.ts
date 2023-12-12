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
    isOverrideConfig: yup.boolean(),
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
    config: configSchema,
});

type WithoutBase<T extends FormDestinationDataBase> = Omit<T, "isEnabled" | "isOverrideConfig" | "config">;
type WithoutAmazonAndBase<T extends S3Destination | GlacierDestination> = Omit<
    WithoutBase<T>,
    "awsAccessKey" | "awsRegionName" | "awsSecretKey" | "remoteFolderName"
>;

const yupRequiredStringForEnabled = yup
    .string()
    .nullable()
    .when(["isEnabled", "config"], {
        is: (isEnabled: boolean, config: BackupConfigurationScript) => isEnabled && !config.isOverrideConfig,
        then: (schema) => schema.required(),
    });

const localSchema = yupObjectSchema<WithoutBase<LocalDestination>>({
    folderPath: yupRequiredStringForEnabled,
}).concat(destinationBaseSchema);

const amazonSchema = yupObjectSchema<AmazonDestination>({
    remoteFolderName: yup.string().nullable(),
    awsRegionName: yup
        .string()
        .nullable()
        .when(["isEnabled", "isUseCustomHost"], {
            is: (isEnabled: boolean, isUseCustomHost: boolean) => isEnabled && !isUseCustomHost,
            then: (schema) =>
                schema
                    .required()
                    .test(
                        "include-dash",
                        "AWS Region must include a '-' and cannot start or end with it",
                        (value) => value && value.includes("-") && !value.startsWith("-") && !value.endsWith("-")
                    ),
        }),
    awsAccessKey: yupRequiredStringForEnabled,
    awsSecretKey: yupRequiredStringForEnabled,
});

const s3Schema = yupObjectSchema<WithoutAmazonAndBase<S3Destination>>({
    bucketName: yupRequiredStringForEnabled
        .min(3)
        .max(63)
        .matches(/^[a-z\d]/, "Bucket name should start with a number or letter")
        .matches(/[a-z\d]$/, "Bucket name should end with a number or letter")
        .matches(/^[a-z0-9.-]+$/, "Allowed characters are lowercase characters, numbers, periods, and dashes")
        .test(
            "dashes-next-to-period",
            'Bucket names cannot contain dashes next to periods (e.g. " -." and/or ".-")',
            (value) => !value || (!value.includes(".-") && !value.includes("-."))
        )
        .test(
            "consecutive-periods",
            "Bucket name cannot contain consecutive periods",
            (value) => !value || !value.includes("..")
        )
        .test(
            "ip",
            "Bucket name must not be formatted as an IP address (e.g., 192.168.5.4)",
            (value) => !value || !/^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/.test(value)
        ),
    forcePathStyle: yup.boolean(),
    isUseCustomHost: yup.boolean(),
    customServerUrl: yup
        .string()
        .nullable()
        .basicUrl()
        .when("isUseCustomHost", { is: true, then: (schema) => schema.required() }),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

const azureSchema = yupObjectSchema<WithoutBase<AzureDestination>>({
    storageContainer: yupRequiredStringForEnabled
        .min(3)
        .max(63)
        .matches(/^[a-z0-9-]+$/, "Allowed characters lowercase characters, numbers and dashes")
        .matches(/^[a-z0-9]/, "Container name must start with a letter or a number")
        .matches(/[a-z0-9]$/, "Container name must end with a letter or a number")
        .test(
            "consecutive-dashes",
            "Consecutive dashes are not permitted in container names",
            (value) => !value || !/--/.test(value)
        ),
    remoteFolderName: yup.string().nullable(),
    accountName: yupRequiredStringForEnabled,
    accountKey: yupRequiredStringForEnabled,
}).concat(destinationBaseSchema);

const googleCloudSchema = yupObjectSchema<WithoutBase<GoogleCloudDestination>>({
    bucketName: yupRequiredStringForEnabled
        .min(3)
        .max(
            222,
            "Bucket name must contain 3 to 63 characters. Names containing dots can contain up to 222 characters, but each dot-separated component can be no longer than 63 characters"
        )
        .matches(
            /^[a-z0-9._-]+$/,
            "Bucket name must contain only lowercase letters, numbers, dashes (-), underscores (_), and dots (.)"
        )
        .matches(/^[a-z0-9]/, "Bucket name must start with a letter or a number")
        .matches(/[a-z0-9]$/, "Bucket name must end with a letter or a number")
        .test(
            "dash-period-underscore",
            "Dashes, periods and underscores are not permitted to be adjacent to another",
            (value) => {
                if (!value) {
                    return true;
                }
                const firstDashRuleRegExp = /\.-/;
                const secondDashRuleRegExp = /\.\./;
                const thirdDashRuleRegExp = /-\./;
                const fourthDashesRegExp = /_\./;
                const fifthDashesRegExp = /\._/;

                return (
                    !firstDashRuleRegExp.test(value) &&
                    !secondDashRuleRegExp.test(value) &&
                    !thirdDashRuleRegExp.test(value) &&
                    !fourthDashesRegExp.test(value) &&
                    !fifthDashesRegExp.test(value)
                );
            }
        ),
    remoteFolderName: yup.string().nullable(),
    googleCredentialsJson: yupRequiredStringForEnabled
        .matches(/"type"/, "Google credentials json is missing 'type' field")
        .matches(/"private_key"/, "Google credentials json is missing 'private_key' field")
        .matches(/"client_email"/, "Google credentials json is missing 'client_email' field"),
}).concat(destinationBaseSchema);

const glacierSchema = yupObjectSchema<WithoutAmazonAndBase<GlacierDestination>>({
    vaultName: yupRequiredStringForEnabled
        .min(1)
        .max(255)
        .matches(
            /^[A-Za-z0-9_.-]+$/,
            "Allowed characters are a-z, A-Z, 0-9, '_' (underscore), '-' (hyphen), and '.' (period)"
        ),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

const ftpSchema = yupObjectSchema<WithoutBase<FtpDestination>>({
    url: yupRequiredStringForEnabled.test("ftp-url", "Url must start with ftp:// or ftps://", (value) => {
        if (!value) {
            return true;
        }
        const lowerCaseValue = value.toLowerCase();
        return lowerCaseValue.startsWith("ftp://") || lowerCaseValue.startsWith("ftps://");
    }),
    userName: yupRequiredStringForEnabled,
    password: yupRequiredStringForEnabled,
    certificateAsBase64: yup
        .string()
        .nullable()
        .when(["isEnabled", "config", "url"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript, url: string) =>
                isEnabled && !config.isOverrideConfig && url && url.toLowerCase().startsWith("ftps://"),
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
