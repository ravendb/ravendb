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

export const allFonts = [
    "JetBrains Mono",
    "Google Sans Code",
    "Fira Code",
    "Source Code Pro",
    "Space Mono",
    "IBM Plex Mono",
];

export const predefinedFontOptions: SelectOption<string>[] = [
    { value: "default", label: "Default" },
    ...allFonts.map((font) => ({ value: font, label: font })),
];
