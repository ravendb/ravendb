import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
import { allStudioEnvironments } from "components/common/studioConfiguration/StudioConfigurationUtils";
import { StudioLanguage, supportedLanguages } from "common/i18n/resources";

const schema = yup
    .object({
        environment: yup.string<StudioEnvironment>().oneOf(allStudioEnvironments),
        replicationFactor: yup.number().nullable().positive().integer(),
        isCollapseDocsWhenOpening: yup.boolean(),
        isSendUsageStats: yup.boolean(),
        tableFont: yup.string().required(),
        monospaceFont: yup.string().required(),
        language: yup
            .string<StudioLanguage>()
            .oneOf([...supportedLanguages])
            .required(),
    })
    .required();

export const studioGlobalConfigurationYupResolver = yupResolver(schema);
export type StudioGlobalConfigurationFormData = yup.InferType<typeof schema>;
