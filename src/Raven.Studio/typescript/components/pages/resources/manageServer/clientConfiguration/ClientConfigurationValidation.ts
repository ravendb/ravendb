import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { ValidationMessageUtils } from "components/utils/ValidationMessageUtils";

const schema = yup
    .object({
        identityPartsSeparatorEnabled: yup.boolean(),
        identityPartsSeparatorValue: yup.string().when("identityPartsSeparatorEnabled", {
            is: true,
            then: (schema) => schema.required(ValidationMessageUtils.required),
        }),
        maximumNumberOfRequestsEnabled: yup.boolean(),
        maximumNumberOfRequestsValue: yup
            .number()
            .positive()
            .integer()
            .when("maximumNumberOfRequestsEnabled", {
                is: true,
                then: (schema) => schema.required(ValidationMessageUtils.required),
            }),
        sessionContextEnabled: yup.boolean(),
        seedEnabled: yup.boolean(),
        seedValue: yup
            .number()
            .positive()
            .integer()
            .when("seedEnabled", {
                is: true,
                then: (schema) => schema.required(ValidationMessageUtils.required),
            }),
        readBalanceBehaviorEnabled: yup.boolean(),
        readBalanceBehaviorValue: yup
            .mixed<Raven.Client.Http.ReadBalanceBehavior>()
            .oneOf(["None", "FastestNode", "RoundRobin"])
            .when("readBalanceBehaviorEnabled", {
                is: true,
                then: (schema) => schema.required(ValidationMessageUtils.required),
            }),
    })
    .required();

export const clientConfigurationYupResolver = yupResolver(schema);
export type ClientConfigurationFormData = yup.InferType<typeof schema>;
