import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { azureSchema, s3Schema } from "components/common/formDestinations/utils/formDestinationsValidation";
import { remoteAttachmentsConstants } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsConstants";

const destinationBaseSchema = yup.object({
    identifier: yup
        .string()
        .test("unique-identifier", "Identifier must be unique across all destinations", function (value) {
            const { options } = this;

            if (options.context?.destinations) {
                const destinations = options.context.destinations as { identifier?: string }[];
                const currentIdentifierInEdit = options.context.currentIdentifier;

                const matches = destinations.filter((dest) => dest.identifier === value);

                if (!currentIdentifierInEdit) {
                    return matches.length === 0;
                }

                if (value === currentIdentifierInEdit) {
                    return matches.length <= 1;
                }

                return matches.length === 0;
            }

            return true;
        })
        .required(),
    disabled: yup.boolean(),
});

const destinationSchema = yup
    .object({
        provider: yup.string().oneOf(remoteAttachmentsConstants.destinationProviderList).required(),
        s3: s3Schema.nullable().when("provider", {
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
