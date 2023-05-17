import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import StudioConfigurationUtils from "./StudioConfigurationUtils";

const schema = yup
    .object({
        overrideConfig: yup.boolean().optional(),
        environmentTagValue: yup
            .mixed<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>()
            .oneOf(StudioConfigurationUtils.allEnvironmentTags)
            .nullable(),
        defaultReplicationFactorValue: yup.number().nullable().positive().integer(),
        collapseDocsWhenOpeningEnabled: yup
            .boolean()
            .optional()
            .when("collapseDocsWhenOpeningEnabled", {
                is: true,
                then: (schema) => schema.required(),
            }),
        sendAnonymousUsageDataEnabled: yup
            .boolean()
            .optional()
            .when("sendAnonymousUsageDataEnabled", {
                is: true,
                then: (schema) => schema.required(),
            }),
    })
    .required();

export const studioConfigurationYupResolver = yupResolver(schema);
export type StudioConfigurationFormData = yup.InferType<typeof schema>;
