import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import ClientConfigurationUtils from "./ClientConfigurationUtils";

const schema = yup
    .object({
        overrideConfig: yup.boolean().optional(),
        identityPartsSeparatorEnabled: yup.boolean().optional(),
        identityPartsSeparatorValue: yup
            .string()
            .nullable()
            .length(1)
            .matches(/[^|]/, { message: "Identity parts separator cannot be set to '|'" })
            .when("identityPartsSeparatorEnabled", {
                is: true,
                then: (schema) => schema.required(),
            }),
        maximumNumberOfRequestsEnabled: yup.boolean().optional(),
        maximumNumberOfRequestsValue: yup
            .number()
            .nullable()
            .positive()
            .integer()
            .when("maximumNumberOfRequestsEnabled", {
                is: true,
                then: (schema) => schema.required(),
            }),
        useSessionContextEnabled: yup.boolean().optional(),
        loadBalancerSeedEnabled: yup.boolean().optional(),
        loadBalancerSeedValue: yup
            .number()
            .nullable()
            .positive()
            .integer()
            .when("seedEnabled", {
                is: true,
                then: (schema) => schema.required(),
            }),
        readBalanceBehaviorEnabled: yup.boolean().optional(),
        readBalanceBehaviorValue: yup
            .mixed<Raven.Client.Http.ReadBalanceBehavior>()
            .oneOf(ClientConfigurationUtils.allReadBalanceBehaviors)
            .nullable()
            .when("readBalanceBehaviorEnabled", {
                is: true,
                then: (schema) => schema.required(),
            }),
    })
    .required();

export const clientConfigurationYupResolver = yupResolver(schema);
export type ClientConfigurationFormData = yup.InferType<typeof schema>;
