const destinationProviderList = ["s3", "azure"] as const;

export type DestinationProvider = (typeof destinationProviderList)[number];

export const remoteAttachmentsConstants = { destinationProviderList };
