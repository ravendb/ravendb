import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { exhaustiveStringTuple } from "components/utils/common";
import StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;

export const allStudioEnvironments = exhaustiveStringTuple<StudioEnvironment>()(
    "None",
    "Development",
    "Testing",
    "Production"
);

const schema = yup
    .object({
        environment: yup.mixed<StudioEnvironment>().oneOf(allStudioEnvironments),
        replicationFactor: yup.number().nullable().positive().integer(),
        isCollapseDocsWhenOpening: yup.boolean(),
        isSendUsageStats: yup.boolean(),
    })
    .required();

export const studioGlobalConfigurationYupResolver = yupResolver(schema);
export type StudioGlobalConfigurationFormData = yup.InferType<typeof schema>;
