import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { azureSchema, s3Schema } from "components/common/formDestinations/utils/formDestinationsValidation";
import { remoteAttachmentsConstants } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsConstants";
import { storageClassOptions } from "components/utils/common";

interface RemoteAttachmentsDestinationContext {
    destinations: { identifier: string }[];
    currentIdentifier?: string;
}

const destinationBaseSchema = yup.object({
    identifier: yup.string().test("unique-identifier", "Identifier must be unique", getIsUniqueIdentifier).required(),
    disabled: yup.boolean(),
});

function getIsUniqueIdentifier(value: string, ctx: yup.TestContext<RemoteAttachmentsDestinationContext>) {
    const { context } = ctx.options;

    if (context.destinations != null) {
        const matches = context.destinations.filter((dest) => dest.identifier === value);

        if (!context.currentIdentifier) {
            return matches.length === 0;
        }

        if (value === context.currentIdentifier) {
            return matches.length <= 1;
        }

        return matches.length === 0;
    }

    return true;
}

const s3StorageClassSchema = yup.object({
    storageClass: yup
        .string()
        .oneOf(storageClassOptions.map((x) => x.value))
        .default("Standard"),
});

const destinationSchema = yup
    .object({
        provider: yup.string().oneOf(remoteAttachmentsConstants.destinationProviderList).required(),
        s3: s3Schema
            .concat(s3StorageClassSchema)
            .nullable()
            .when("provider", {
                is: "s3",
                then: (schema) => schema.required(),
                otherwise: (schema) => schema.nullable(),
            }),
        azure: azureSchema.nullable().when("provider", {
            is: "azure",
            then: (schema) => schema.required(),
            otherwise: (schema) => schema.nullable(),
        }),
    })
    .concat(destinationBaseSchema);

const schema = yup.object({
    isRemoteAttachmentsEnabled: yup.boolean(),
    isCheckFrequencyInSecEnabled: yup.boolean(),
    checkFrequencyInSec: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isCheckFrequencyInSecEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    isMaxItemsToProcessEnabled: yup.boolean(),
    maxItemsToProcess: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isMaxItemsToProcessEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    isConcurrentUploadsEnabled: yup.boolean(),
    concurrentUploads: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isConcurrentUploadsEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

export const remoteAttachmentsDestinationYupResolver = yupResolver(destinationSchema);
export type RemoteAttachmentsDestinationFormData = yup.InferType<typeof destinationSchema>;

export const remoteAttachmentsYupResolver = yupResolver(schema);
export type RemoteAttachmentsFormData = yup.InferType<typeof schema>;
