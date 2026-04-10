import { exhaustiveStringTuple } from "components/utils/common";
import StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
import { SelectOption } from "../select/Select";

export const allStudioEnvironments = exhaustiveStringTuple<StudioEnvironment>()(
    "None",
    "Development",
    "Testing",
    "Production"
);

export const studioEnvironmentOptions: SelectOption<StudioEnvironment>[] = allStudioEnvironments.map((environment) => ({
    value: environment,
    label: environment,
}));

export const virtualTableFontOptions: SelectOption<string>[] = [
    { value: "default", label: "Default" },
    { value: "Inter", label: "Inter" },
    { value: "JetBrains Mono", label: "JetBrains Mono" },
    { value: "Google Sans Code", label: "Google Sans Code" },
    { value: "Fira Code", label: "Fira Code" },
    { value: "Source Code Pro", label: "Source Code Pro" },
    { value: "Space Mono", label: "Space Mono" },
    { value: "IBM Plex Mono", label: "IBM Plex Mono" },
];

export const monospaceFontOptions: SelectOption<string>[] = [
    { value: "default", label: "Default" },
    { value: "JetBrains Mono", label: "JetBrains Mono" },
    { value: "Google Sans Code", label: "Google Sans Code" },
    { value: "Fira Code", label: "Fira Code" },
    { value: "Source Code Pro", label: "Source Code Pro" },
    { value: "Space Mono", label: "Space Mono" },
    { value: "IBM Plex Mono", label: "IBM Plex Mono" },
];
