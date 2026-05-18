import RemoteAttachmentsConfiguration = Raven.Client.Documents.Attachments.RemoteAttachmentsConfiguration;
import RemoteAttachmentsDestinationConfiguration = Raven.Client.Documents.Attachments.RemoteAttachmentsDestinationConfiguration;
import S3StorageClass = Raven.Client.Documents.Operations.Backups.S3StorageClass;
import {
    RemoteAttachmentsDestinationFormData,
    RemoteAttachmentsFormData,
} from "components/pages/database/settings/remoteAttachments/remoteAttachmentsValidation";
import {
    defaultAzureFormData,
    defaultS3FormData,
} from "components/common/formDestinations/utils/formDestinationsMapsFromDto";
import { mapAmazonToDto, mapAzureToDto } from "components/common/formDestinations/utils/formDestinationsMapsToDto";
import IconName from "../../../../../../typings/server/icons";
import { DestinationProvider } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsConstants";
import { S3Destination } from "components/common/formDestinations/utils/formDestinationsTypes";

type RemoteAttachmentsFormDataWithDestinations = RemoteAttachmentsFormData & {
    destinations: RemoteAttachmentsDestinationFormData[];
};

function mapToDto(form: RemoteAttachmentsFormDataWithDestinations): RemoteAttachmentsConfiguration {
    const destinations: Record<string, RemoteAttachmentsDestinationConfiguration> = {};

    for (const dest of form.destinations ?? []) {
        const s3Settings = dest.provider === "s3" ? mapS3ToDto({ ...dest.s3, isEnabled: true }) : undefined;
        const azureSettings = dest.provider === "azure" ? mapAzureToDto({ ...dest.azure, isEnabled: true }) : undefined;

        destinations[dest.identifier] = {
            Disabled: dest.disabled,
            S3Settings: s3Settings,
            AzureSettings: azureSettings,
        };
    }

    return {
        Disabled: !form.isRemoteAttachmentsEnabled,
        CheckFrequencyInSec: form.isCheckFrequencyInSecEnabled ? form.checkFrequencyInSec : null,
        MaxItemsToProcess: form.isMaxItemsToProcessEnabled ? form.maxItemsToProcess : null,
        Destinations: destinations,
        ConcurrentUploads: form.isConcurrentUploadsEnabled ? form.concurrentUploads : null,
    };
}

function mapFromDto(dto: RemoteAttachmentsConfiguration): RemoteAttachmentsFormDataWithDestinations {
    if (!dto) {
        return {
            isRemoteAttachmentsEnabled: false,
            isCheckFrequencyInSecEnabled: false,
            checkFrequencyInSec: null,
            concurrentUploads: null,
            isMaxItemsToProcessEnabled: false,
            maxItemsToProcess: null,
            isConcurrentUploadsEnabled: false,
            destinations: [],
        };
    }

    const destinations: RemoteAttachmentsDestinationFormData[] = [];

    if (dto.Destinations) {
        for (const [destinationIdentifier, destinationConfig] of Object.entries(dto.Destinations)) {
            if (destinationConfig.S3Settings) {
                const s3Config = destinationConfig.S3Settings;
                destinations.push({
                    provider: "s3",
                    identifier: destinationIdentifier,
                    disabled: !!destinationConfig.Disabled,
                    s3: {
                        ...defaultS3FormData,
                        awsAccessKey: s3Config.AwsAccessKey,
                        awsSecretKey: s3Config.AwsSecretKey,
                        awsRegionName: s3Config.AwsRegionName,
                        awsSessionToken: s3Config.AwsSessionToken,
                        remoteFolderName: s3Config.RemoteFolderName,
                        bucketName: s3Config.BucketName,
                        isUseCustomHost: s3Config.CustomServerUrl != null,
                        customServerUrl: s3Config.CustomServerUrl,
                        forcePathStyle: s3Config.ForcePathStyle,
                        disableChecksumValidation: s3Config.DisableChecksumValidation,
                        storageClass: s3Config.StorageClass,
                    },
                    azure: null,
                });
            } else if (destinationConfig.AzureSettings) {
                const azureConfig = destinationConfig.AzureSettings;
                destinations.push({
                    provider: "azure",
                    identifier: destinationIdentifier,
                    disabled: !!destinationConfig.Disabled,
                    azure: {
                        ...defaultAzureFormData,
                        accountName: azureConfig.AccountName,
                        accountKey: azureConfig.AccountKey,
                        storageContainer: azureConfig.StorageContainer,
                        remoteFolderName: azureConfig.RemoteFolderName,
                    },
                    s3: null,
                });
            }
        }
    }

    return {
        isRemoteAttachmentsEnabled: !dto.Disabled,
        isCheckFrequencyInSecEnabled: dto.CheckFrequencyInSec != null,
        checkFrequencyInSec: dto.CheckFrequencyInSec,
        isMaxItemsToProcessEnabled: dto.MaxItemsToProcess != null,
        maxItemsToProcess: dto.MaxItemsToProcess,
        isConcurrentUploadsEnabled: dto.ConcurrentUploads != null,
        concurrentUploads: dto.ConcurrentUploads,
        destinations,
    };
}

const getProviderIcon = (provider: string): IconName => {
    switch (provider) {
        case "s3":
            return "aws";
        case "azure":
            return "azure";
        default:
            return null;
    }
};

const getProviderName = (provider: DestinationProvider) => {
    switch (provider) {
        case "s3":
            return "Amazon S3";
        case "azure":
            return "Azure";
        default:
            return null;
    }
};

interface RemoteAttachmentsS3Destination extends S3Destination {
    storageClass?: S3StorageClass;
}

const mapS3ToDto = (
    destination: RemoteAttachmentsS3Destination
): Raven.Client.Documents.Attachments.RemoteAttachmentsS3Settings => {
    const customServerUrl =
        !destination.config.isOverrideConfig && destination.isUseCustomHost ? destination.customServerUrl : undefined;

    const forcePathStyle =
        !destination.config.isOverrideConfig && destination.isUseCustomHost ? destination.forcePathStyle : undefined;

    const disableChecksumValidation =
        !destination.config.isOverrideConfig && destination.isUseCustomHost
            ? destination.disableChecksumValidation
            : undefined;

    return {
        ...mapAmazonToDto(destination),
        CustomServerUrl: customServerUrl,
        ForcePathStyle: forcePathStyle,
        DisableChecksumValidation: disableChecksumValidation,
        BucketName: destination.bucketName,
        StorageClass: destination.storageClass,
    };
};

export const remoteAttachmentsUtils = {
    mapToDto,
    mapFromDto,
    getProviderIcon,
    getProviderName,
    mapS3ToDto,
};
