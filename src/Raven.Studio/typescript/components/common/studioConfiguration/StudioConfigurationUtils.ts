import { StudioConfigurationFormData } from "./StudioConfigurationValidation";
import StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;
import { exhaustiveStringTuple } from "components/utils/common";

export default class StudioConfigurationUtils {
    static allEnvironmentTags = exhaustiveStringTuple<StudioConfiguration.StudioEnvironment>()(
        "Development",
        "None",
        "Production",
        "Testing"
    );

    static formatEnvironmentTag(environmentTag: StudioConfiguration.StudioEnvironment) {
        switch (environmentTag) {
            case "Testing":
                return "Testing";
            case "Development":
                return "Development";
            case "Production":
                return "Production";
            default:
                return "None";
        }
    }

    static getEnvironmentTagOptions(): valueAndLabelItem<StudioConfiguration.StudioEnvironment, string>[] {
        return StudioConfigurationUtils.allEnvironmentTags.map((value) => ({
            value,
            label: StudioConfigurationUtils.formatEnvironmentTag(value),
        }));
    }

    static mapToFormData(dto: StudioConfiguration, isGlobal: boolean): StudioConfigurationFormData {
        if (!dto) {
            return {
                overrideConfig: isGlobal,
                environmentTagValue: "None",
                defaultReplicationFactorValue: null,
                collapseDocsWhenOpeningEnabled: false,
                sendAnonymousUsageDataEnabled: false,
            };
        }

        return {
            overrideConfig: dto.Disabled,
            environmentTagValue: dto.Environment || "None",
            defaultReplicationFactorValue: dto.DefaultReplicationFactor,
        };
    }

    static mapToDto(formData: StudioConfigurationFormData, isGlobal: boolean): StudioConfiguration {
        if (!formData.overrideConfig && !isGlobal) {
            return {
                Disabled: true,
                Environment: undefined,
                DisableAutoIndexCreation: false,
                DefaultReplicationFactor: null,
                CollapseDocsWhenOpeningEnabled: false,
                SendAnonymousUsageDataEnabled: false,
            };
        }

        return {
            Environment: formData.environmentTagValue,
            DefaultReplicationFactor: formData.defaultReplicationFactorValue,
            Disabled: !formData.overrideConfig,
            DisableAutoIndexCreation: false,
            CollapseDocsWhenOpeningEnabled: formData.collapseDocsWhenOpeningEnabled,
            SendAnonymousUsageDataEnabled: formData.sendAnonymousUsageDataEnabled,
        };
    }
}
