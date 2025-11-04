import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { azureSchema, s3Schema } from "components/common/formDestinations/utils/formDestinationsValidation";

const destinationBaseSchema = yup.object({
    identifier: yup
        .string()
        .test("unique-identifier", "Identifier must be unique across all destinations", function (value) {
            const { options } = this;

            if (options.context?.destinations) {
                const destinations = options.context.destinations as Array<{ identifier?: string }>;
                const currentIdentifierInEdit = options.context.currentIdentifier;

                const matches = destinations.filter((dest) => dest.identifier === value);

                // Creating new: require no matches
                if (!currentIdentifierInEdit) {
                    return matches.length === 0;
                }

                // Editing: allow if the only match is the one we're editing
                if (value === currentIdentifierInEdit) {
                    return matches.length <= 1;
                }

                // Renaming to a different value must be unique
                return matches.length === 0;
            }

            return true;
        }),
    disabled: yup.boolean(),
});

const destinationSchema = yup
    .object({
        provider: yup.string().oneOf(["s3", "azure"]).required(),
        // Important: allow the non-selected provider object to be null
        s3: s3Schema
            .nullable()
            .when("provider", {
                is: "s3",
                then: (schema) => schema.required(),
                otherwise: (schema) => schema.nullable(),
            }),
        azure: azureSchema
            .nullable()
            .when("provider", {
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
    // destinations: yup.array().of(destinationSchema),
});

export const remoteAttachmentsDestinationYupResolver = yupResolver(destinationSchema);
export type RemoteAttachmentsDestinationFormData = yup.InferType<typeof destinationSchema>;

export const remoteAttachmentsYupResolver = yupResolver(schema);
export type RemoteAttachmentsFormData = yup.InferType<typeof schema>;
