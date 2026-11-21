import IconName from "typings/server/icons";
import { StudioTranslate } from "common/i18n/resources";
import { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { storageClassOptions } from "components/utils/common";
import { DestinationType, DestinationsDto, FormDestinationData, FormDestinations } from "./formDestinationsTypes";
import {
    mapAzureToDto,
    mapFtpToDto,
    mapGlacierToDto,
    mapGoogleCloudToDto,
    mapLocalToDto,
    mapS3ToDto,
} from "./formDestinationsMapsToDto";
import {
    defaultAzureFormData,
    defaultFtpFormData,
    defaultGlacierFormData,
    defaultGoogleCloudFormData,
    defaultLocalFormData,
    defaultS3FormData,
} from "./formDestinationsMapsFromDto";

type PeriodicBackupConnectionType = Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType;
type BackupSettings = Raven.Client.Documents.Operations.Backups.BackupSettings;
type S3StorageClass = Raven.Client.Documents.Operations.Backups.S3StorageClass;

const DEFAULT_S3_STORAGE_CLASS: S3StorageClass = "Standard";
const CLOUD_DESTINATIONS_LICENSE: LicenseBadgeText = "Professional +";

interface DestinationTypeInfo {
    icon: IconName;
    allowedDestinationName: backupOptions;
    connectionType: PeriodicBackupConnectionType;
    dtoKey: keyof DestinationsDto;
    isCloud: boolean;
    defaultData: FormDestinationData;
    mapToDto(data: FormDestinationData): BackupSettings;
}

const DESTINATION_TYPES: DestinationType[] = ["local", "s3", "azure", "googleCloud", "glacier", "ftp"];

const DESTINATION_TYPE_INFO: Record<DestinationType, DestinationTypeInfo> = {
    local: {
        icon: "folder",
        allowedDestinationName: "Local",
        connectionType: "Local",
        dtoKey: "LocalSettings",
        isCloud: false,
        defaultData: defaultLocalFormData,
        mapToDto: mapLocalToDto,
    },
    s3: {
        icon: "aws",
        allowedDestinationName: "AmazonS3",
        connectionType: "S3",
        dtoKey: "S3Settings",
        isCloud: true,
        defaultData: defaultS3FormData,
        mapToDto: mapS3ToDto,
    },
    azure: {
        icon: "azure",
        allowedDestinationName: "Azure",
        connectionType: "Azure",
        dtoKey: "AzureSettings",
        isCloud: true,
        defaultData: defaultAzureFormData,
        mapToDto: mapAzureToDto,
    },
    googleCloud: {
        icon: "cloud",
        allowedDestinationName: "GoogleCloud",
        connectionType: "GoogleCloud",
        dtoKey: "GoogleCloudSettings",
        isCloud: true,
        defaultData: defaultGoogleCloudFormData,
        mapToDto: mapGoogleCloudToDto,
    },
    glacier: {
        icon: "aws",
        allowedDestinationName: "AmazonGlacier",
        connectionType: "Glacier",
        dtoKey: "GlacierSettings",
        isCloud: true,
        defaultData: defaultGlacierFormData,
        mapToDto: mapGlacierToDto,
    },
    ftp: {
        icon: "server",
        allowedDestinationName: "FTP",
        connectionType: "FTP",
        dtoKey: "FtpSettings",
        isCloud: true,
        defaultData: defaultFtpFormData,
        mapToDto: mapFtpToDto,
    },
};

function getEnabledDestinationTypes(destinations: FormDestinations["destinations"]): DestinationType[] {
    return DESTINATION_TYPES.filter((type) => destinations?.[type]?.isEnabled);
}

function getIdentifyingSetting(type: DestinationType, destinations: FormDestinations["destinations"]): string {
    switch (type) {
        case "local":
            return destinations.local.folderPath;
        case "s3":
            return destinations.s3.bucketName;
        case "azure":
            return destinations.azure.storageContainer;
        case "googleCloud":
            return destinations.googleCloud.bucketName;
        case "glacier":
            return destinations.glacier.vaultName;
        case "ftp":
            return destinations.ftp.url;
    }
}

function hasDestinationSettings(type: DestinationType, destinations: FormDestinations["destinations"]): boolean {
    const { config } = destinations[type];
    const hasScript = !!config?.isOverrideConfig && !!config.exec?.trim();

    return hasScript || !!getIdentifyingSetting(type, destinations)?.trim();
}

function isDestinationAdded(type: DestinationType, destinations: FormDestinations["destinations"]): boolean {
    return !!destinations[type].isEnabled || hasDestinationSettings(type, destinations);
}

function getAddedDestinationTypes(destinations: FormDestinations["destinations"]): DestinationType[] {
    return DESTINATION_TYPES.filter((type) => isDestinationAdded(type, destinations));
}

function mapAddedDestinationToDto<TType extends DestinationType, TDto extends BackupSettings>(
    type: TType,
    destinations: FormDestinations["destinations"],
    mapToDto: (destination: FormDestinations["destinations"][TType]) => TDto
): TDto {
    if (!isDestinationAdded(type, destinations)) {
        return undefined;
    }

    const destination = destinations[type];

    return { ...mapToDto({ ...destination, isEnabled: true }), Disabled: !destination.isEnabled };
}

function mapAddedDestinationsToDto(destinations: FormDestinations["destinations"]): DestinationsDto {
    return {
        LocalSettings: mapAddedDestinationToDto("local", destinations, mapLocalToDto),
        S3Settings: mapAddedDestinationToDto("s3", destinations, mapS3ToDto),
        AzureSettings: mapAddedDestinationToDto("azure", destinations, mapAzureToDto),
        GoogleCloudSettings: mapAddedDestinationToDto("googleCloud", destinations, mapGoogleCloudToDto),
        GlacierSettings: mapAddedDestinationToDto("glacier", destinations, mapGlacierToDto),
        FtpSettings: mapAddedDestinationToDto("ftp", destinations, mapFtpToDto),
    };
}

function isDestinationAllowed(type: DestinationType, allowedDestinations: backupOptions[] | null | undefined): boolean {
    if (!allowedDestinations) {
        return true;
    }

    return allowedDestinations.includes(DESTINATION_TYPE_INFO[type].allowedDestinationName);
}

function isDestinationLicensed(type: DestinationType, hasCloudBackups: boolean): boolean {
    return !DESTINATION_TYPE_INFO[type].isCloud || hasCloudBackups;
}

function isCredentialsTestAvailable(type: DestinationType, destination: FormDestinationData): boolean {
    return type !== "local" && !destination?.config?.isOverrideConfig;
}

export interface DestinationDetail {
    label: string;
    value: string;
}

type Translate = StudioTranslate<"formDestinations">;

function getAuthenticationMethodLabel(destination: FormDestinationData, t: Translate): string {
    return destination?.config?.isOverrideConfig ? t("externalScript") : t("staticCredentials");
}

function getStaticCredentialsDetails(
    type: DestinationType,
    data: FormDestinations["destinations"],
    t: Translate
): DestinationDetail[] {
    switch (type) {
        case "local":
            return [{ label: t("local.backupDirectory"), value: data.local.folderPath }];
        case "s3":
            return [
                {
                    label: t("s3.provider"),
                    value: data.s3.isUseCustomHost ? t("s3.providerCustom") : t("s3.providerAws"),
                },
                ...(data.s3.isUseCustomHost
                    ? [{ label: t("s3.customServerUrl"), value: data.s3.customServerUrl }]
                    : []),
                { label: t("aws.region"), value: data.s3.awsRegionName },
                { label: t("s3.bucketName"), value: data.s3.bucketName },
                { label: t("remoteFolderName"), value: data.s3.remoteFolderName },
                {
                    label: t("s3.storageClass"),
                    value: storageClassOptions.find((x) => x.value === data.s3.storageClass)?.label,
                },
            ];
        case "azure":
            return [
                { label: t("azure.accountName"), value: data.azure.accountName },
                { label: t("azure.storageContainer"), value: data.azure.storageContainer },
                { label: t("remoteFolderName"), value: data.azure.remoteFolderName },
            ];
        case "googleCloud":
            return [
                { label: t("googleCloud.bucket"), value: data.googleCloud.bucketName },
                { label: t("remoteFolderName"), value: data.googleCloud.remoteFolderName },
            ];
        case "glacier":
            return [
                { label: t("aws.region"), value: data.glacier.awsRegionName },
                { label: t("glacier.vaultName"), value: data.glacier.vaultName },
                { label: t("remoteFolderName"), value: data.glacier.remoteFolderName },
            ];
        case "ftp":
            return [{ label: t("ftp.host"), value: data.ftp.url }];
    }
}

function getDestinationDetails(
    type: DestinationType,
    data: FormDestinations["destinations"],
    t: Translate
): DestinationDetail[] {
    const destination = data[type];
    const details: DestinationDetail[] = destination.config?.isOverrideConfig
        ? [
              { label: t("script.exec"), value: destination.config.exec },
              { label: t("script.arguments"), value: destination.config.arguments },
              {
                  label: t("script.timeout"),
                  value:
                      destination.config.timeoutInMs != null
                          ? `${destination.config.timeoutInMs.toLocaleString()} ms`
                          : null,
              },
          ]
        : getStaticCredentialsDetails(type, data, t);

    return [
        { label: t("authenticationMethod"), value: getAuthenticationMethodLabel(destination, t) },
        ...details.filter((x) => x.value),
    ];
}

export const formDestinationsUtils = {
    DESTINATION_TYPES,
    DESTINATION_TYPE_INFO,
    DEFAULT_S3_STORAGE_CLASS,
    CLOUD_DESTINATIONS_LICENSE,
    getEnabledDestinationTypes,
    getAddedDestinationTypes,
    mapAddedDestinationsToDto,
    isDestinationAllowed,
    isDestinationLicensed,
    isCredentialsTestAvailable,
    getAuthenticationMethodLabel,
    getDestinationDetails,
};
