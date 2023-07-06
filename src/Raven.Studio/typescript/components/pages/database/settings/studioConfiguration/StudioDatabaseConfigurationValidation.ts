import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
import StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;
import { allStudioEnvironments } from "components/common/studioConfiguration/StudioConfigurationUtils";
import { yupObjectSchema } from "components/utils/yupUtils";

const schema = yupObjectSchema<StudioConfiguration>({
    Environment: yup.string<StudioEnvironment>().oneOf(allStudioEnvironments),
    DisableAutoIndexCreation: yup.boolean(),
    Disabled: yup.boolean(),
});

export const studioDatabaseConfigurationYupResolver = yupResolver(schema);
export type StudioDatabaseConfigurationFormData = Required<yup.InferType<typeof schema>>;
