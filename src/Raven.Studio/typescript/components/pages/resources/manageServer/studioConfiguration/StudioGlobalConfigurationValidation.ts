import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
import { allStudioEnvironments } from "components/common/studioConfiguration/StudioConfigurationUtils";

const schema = yup
    .object({
        environment: yup.string<StudioEnvironment>().oneOf(allStudioEnvironments),
        replicationFactor: yup.number().nullable().positive().integer(),
        isCollapseDocsWhenOpening: yup.boolean(),
        isSendUsageStats: yup.boolean(),
        tableFont: yup.string().required(),
        monospaceFont: yup.string().required(),
    })
    .required();

export const studioGlobalConfigurationYupResolver = yupResolver(schema);
export type StudioGlobalConfigurationFormData = yup.InferType<typeof schema>;
