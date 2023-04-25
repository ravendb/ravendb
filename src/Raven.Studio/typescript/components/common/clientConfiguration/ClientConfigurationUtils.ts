import { ClientConfigurationFormData } from "./ClientConfigurationValidation";
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;

export default class ClientConfigurationUtils {
    static mapToFormData(dto: ClientConfiguration): ClientConfigurationFormData {
        if (!dto) {
            return null;
        }

        return {
            overrideConfig: !dto.Disabled,
            identityPartsSeparatorEnabled: !!dto.IdentityPartsSeparator,
            identityPartsSeparatorValue: dto.IdentityPartsSeparator,
            maximumNumberOfRequestsEnabled: !!dto.MaxNumberOfRequestsPerSession,
            maximumNumberOfRequestsValue: dto.MaxNumberOfRequestsPerSession,
            useSessionContextEnabled: dto.LoadBalanceBehavior === "UseSessionContext",
            loadBalancerSeedEnabled: !!dto.LoadBalancerContextSeed,
            loadBalancerSeedValue: dto.LoadBalancerContextSeed,
            readBalanceBehaviorEnabled: !!dto.ReadBalanceBehavior && dto.ReadBalanceBehavior !== "None",
            readBalanceBehaviorValue: dto.ReadBalanceBehavior,
        };
    }

    static mapToDto(formData: ClientConfigurationFormData): ClientConfiguration {
        if (!formData.overrideConfig) {
            return {
                Disabled: true,
                Etag: undefined,
            };
        }

        return {
            IdentityPartsSeparator: formData.identityPartsSeparatorValue,
            LoadBalanceBehavior: formData.useSessionContextEnabled ? "UseSessionContext" : "None",
            LoadBalancerContextSeed: formData.loadBalancerSeedValue,
            ReadBalanceBehavior: formData.readBalanceBehaviorValue,
            MaxNumberOfRequestsPerSession: formData.maximumNumberOfRequestsValue,
            Disabled: !formData.overrideConfig,
            Etag: undefined,
        };
    }
}
