import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import ClientConfigurationUtils from "./ClientConfigurationUtils";

const schema = yup
    .object({
        overrideConfig: yup.boolean().optional(),
        identityPartsSeparatorValue: yup
            .string()
            .nullable()
            .length(1)
            .matches(/[^|]/, { message: "Identity parts separator cannot be set to '|'" }),
        maximumNumberOfRequestsValue: yup.number().nullable().positive().integer(),
        loadBalancerValue: yup
            .mixed<Raven.Client.Http.LoadBalanceBehavior>()
            .oneOf(ClientConfigurationUtils.allLoadBalanceBehaviors)
            .nullable(),
        loadBalancerSeedValue: yup.number().nullable().positive().integer(),
        readBalanceBehaviorValue: yup
            .mixed<Raven.Client.Http.ReadBalanceBehavior>()
            .oneOf(ClientConfigurationUtils.allReadBalanceBehaviors)
            .nullable(),
    })
    .required();

export const clientConfigurationYupResolver = yupResolver(schema);
export type ClientConfigurationFormData = yup.InferType<typeof schema>;
