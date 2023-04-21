import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { ValidationMessageUtils as Messages } from "components/utils/ValidationMessageUtils";

const schema = yup
    .object({
        identityPartsSeparatorEnabled: yup.boolean().optional(),
        identityPartsSeparatorValue: yup
            .string()
            .nullable()
            .length(1, "Enter one character only")
            .matches(/[^|]/, { message: "Identity parts separator cannot be set to '|'" })
            .when("identityPartsSeparatorEnabled", {
                is: true,
                then: (schema) => schema.required(Messages.required),
            }),
        maximumNumberOfRequestsEnabled: yup.boolean().optional(),
        maximumNumberOfRequestsValue: yup
            .number()
            .nullable()
            .typeError(Messages.number)
            .positive(Messages.positiveNumber)
            .integer(Messages.integerNumber)
            .when("maximumNumberOfRequestsEnabled", {
                is: true,
                then: (schema) => schema.required(Messages.required),
            }),
        useSessionContextEnabled: yup.boolean().optional(),
        loadBalancerSeedEnabled: yup.boolean().optional(),
        loadBalancerSeedValue: yup
            .number()
            .nullable()
            .typeError(Messages.number)
            .positive(Messages.positiveNumber)
            .integer(Messages.integerNumber)
            .when("seedEnabled", {
                is: true,
                then: (schema) => schema.required(Messages.required),
            }),
        readBalanceBehaviorEnabled: yup.boolean().optional(),
        readBalanceBehaviorValue: yup
            .mixed<Raven.Client.Http.ReadBalanceBehavior>()
            .oneOf(["None", "FastestNode", "RoundRobin"])
            .nullable()
            .when("readBalanceBehaviorEnabled", {
                is: true,
                then: (schema) => schema.required(Messages.required),
            }),
    })
    .required();

export const clientConfigurationYupResolver = yupResolver(schema);
export type ClientConfigurationFormData = yup.InferType<typeof schema>;
