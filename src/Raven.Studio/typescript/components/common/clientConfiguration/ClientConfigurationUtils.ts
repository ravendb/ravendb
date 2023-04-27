import { ClientConfigurationFormData } from "./ClientConfigurationValidation";
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;
import assertUnreachable from "components/utils/assertUnreachable";
import { exhaustiveStringTuple } from "components/utils/common";

export default class ClientConfigurationUtils {
    static allReadBalanceBehaviors = exhaustiveStringTuple<ReadBalanceBehavior>()("None", "RoundRobin", "FastestNode");

    static formatReadBalanceBehavior(readBalanceBehavior: ReadBalanceBehavior) {
        switch (readBalanceBehavior) {
            case "None":
                return "None";
            case "RoundRobin":
                return "Round Robin";
            case "FastestNode":
                return "Fastest node";
            default:
                assertUnreachable(readBalanceBehavior);
        }
    }

    static getReadBalanceBehaviorOptions(): valueAndLabelItem<ReadBalanceBehavior, string>[] {
        return ClientConfigurationUtils.allReadBalanceBehaviors.map((value) => ({
            value,
            label: ClientConfigurationUtils.formatReadBalanceBehavior(value),
        }));
    }

    static mapToFormData(dto: ClientConfiguration, isGlobal: boolean): ClientConfigurationFormData {
        if (!dto) {
            return {
                overrideConfig: isGlobal,
                identityPartsSeparatorEnabled: false,
                identityPartsSeparatorValue: null,
                maximumNumberOfRequestsEnabled: false,
                maximumNumberOfRequestsValue: null,
                useSessionContextEnabled: false,
                loadBalancerSeedEnabled: false,
                loadBalancerSeedValue: null,
                readBalanceBehaviorEnabled: false,
                readBalanceBehaviorValue: null,
            };
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

    static mapToDto(formData: ClientConfigurationFormData, isGlobal: boolean): ClientConfiguration {
        if (!formData.overrideConfig && !isGlobal) {
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
