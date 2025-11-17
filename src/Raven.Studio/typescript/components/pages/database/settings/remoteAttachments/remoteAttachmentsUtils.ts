import RemoteAttachmentsConfiguration = Raven.Client.Documents.Attachments.RemoteAttachmentsConfiguration;
import RemoteAttachmentsDestinationConfiguration = Raven.Client.Documents.Attachments.RemoteAttachmentsDestinationConfiguration;
import {
    RemoteAttachmentsDestinationFormData,
    RemoteAttachmentsFormData,
} from "components/pages/database/settings/remoteAttachments/remoteAttachmentsValidation";
import {
    defaultAzureFormData,
    defaultS3FormData,
} from "components/common/formDestinations/utils/formDestinationsMapsFromDto";
import { mapAzureToDto, mapS3ToDto } from "components/common/formDestinations/utils/formDestinationsMapsToDto";

type RemoteAttachmentsFormDataWithDestinations = RemoteAttachmentsFormData & {
    destinations: RemoteAttachmentsDestinationFormData[];
};

function mapToDto(form: RemoteAttachmentsFormDataWithDestinations): RemoteAttachmentsConfiguration {
    const destinations: Record<string, RemoteAttachmentsDestinationConfiguration> = {};

    for (const dest of form.destinations ?? []) {
        const s3Settings = dest.provider === "s3" ? mapS3ToDto({ ...dest.s3, isEnabled: true }) : undefined;
        const azureSettings = dest.provider === "azure" ? mapAzureToDto({ ...dest.azure, isEnabled: true }) : undefined;

        destinations[dest.identifier] = {
            Identifier: dest.identifier,
            Disabled: dest.disabled,
            S3Settings: s3Settings,
            AzureSettings: azureSettings,
        } as RemoteAttachmentsDestinationConfiguration;
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
            destinations: [],
        };
    }

    const destinations: RemoteAttachmentsDestinationFormData[] = [];

    if (dto.Destinations) {
        for (const [_key, destinationConfig] of Object.entries(dto.Destinations)) {
            if (destinationConfig.S3Settings) {
                const {
                    AwsAccessKey,
                    AwsSecretKey,
                    AwsRegionName,
                    AwsSessionToken,
                    RemoteFolderName,
                    BucketName,
                    CustomServerUrl,
                    ForcePathStyle,
                } = destinationConfig.S3Settings;
                destinations.push({
                    provider: "s3",
                    identifier: destinationConfig.Identifier,
                    disabled: !!destinationConfig.Disabled,
                    s3: {
                        ...defaultS3FormData,
                        awsAccessKey: AwsAccessKey,
                        awsSecretKey: AwsSecretKey,
                        awsRegionName: AwsRegionName,
                        awsSessionToken: AwsSessionToken,
                        remoteFolderName: RemoteFolderName,
                        bucketName: BucketName,
                        customServerUrl: CustomServerUrl,
                        forcePathStyle: ForcePathStyle,
                    },
                    azure: null,
                });
            } else if (destinationConfig.AzureSettings) {
                const { AccountName, AccountKey, StorageContainer, RemoteFolderName } = destinationConfig.AzureSettings;
                destinations.push({
                    provider: "azure",
                    identifier: destinationConfig.Identifier,
                    disabled: !!destinationConfig.Disabled,
                    azure: {
                        ...defaultAzureFormData,
                        accountName: AccountName,
                        accountKey: AccountKey,
                        storageContainer: StorageContainer,
                        remoteFolderName: RemoteFolderName,
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
        destinations,
    };
}

export const remoteAttachmentsUtils = {
    mapToDto,
    mapFromDto,
};
