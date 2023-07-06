import { exhaustiveStringTuple } from "components/utils/common";
import StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
import { SelectOption } from "../Select";

export const allStudioEnvironments = exhaustiveStringTuple<StudioEnvironment>()(
    "None",
    "Development",
    "Testing",
    "Production"
);

export const environmentOptions: SelectOption<StudioEnvironment>[] = allStudioEnvironments.map((environment) => ({
    value: environment,
    label: environment,
}));
