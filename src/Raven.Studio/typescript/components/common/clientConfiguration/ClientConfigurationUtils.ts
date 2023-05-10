import { ClientConfigurationFormData } from "./ClientConfigurationValidation";
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;
import LoadBalanceBehavior = Raven.Client.Http.LoadBalanceBehavior;
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

    static allLoadBalanceBehaviors = exhaustiveStringTuple<LoadBalanceBehavior>()("None", "UseSessionContext");

    static formatLoadBalanceBehavior(loadBalanceBehavior: LoadBalanceBehavior) {
        switch (loadBalanceBehavior) {
            case "None":
                return "None";
            case "UseSessionContext":
                return "Use Session Context";
            default:
                assertUnreachable(loadBalanceBehavior);
        }
    }

    static getLoadBalanceBehaviorOptions(): valueAndLabelItem<LoadBalanceBehavior, string>[] {
        return ClientConfigurationUtils.allLoadBalanceBehaviors.map((value) => ({
            value,
            label: ClientConfigurationUtils.formatLoadBalanceBehavior(value),
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
                loadBalancerEnabled: false,
                loadBalancerValue: null,
                loadBalancerSeedEnabled: false,
                loadBalancerSeedValue: null,
                readBalanceBehaviorEnabled: false,
                readBalanceBehaviorValue: null,
            };
        }

        return {
            overrideConfig: !dto.Disabled,
            identityPartsSeparatorEnabled: !!dto.IdentityPartsSeparator,
            identityPartsSeparatorValue: dto.IdentityPartsSeparator || null,
            maximumNumberOfRequestsEnabled: !!dto.MaxNumberOfRequestsPerSession,
            maximumNumberOfRequestsValue: dto.MaxNumberOfRequestsPerSession || null,
            loadBalancerEnabled: dto.LoadBalanceBehavior === "UseSessionContext",
            loadBalancerValue: dto.LoadBalanceBehavior || "None",
            loadBalancerSeedEnabled: !!dto.LoadBalancerContextSeed,
            loadBalancerSeedValue: dto.LoadBalancerContextSeed || null,
            readBalanceBehaviorEnabled: !!dto.ReadBalanceBehavior && dto.ReadBalanceBehavior !== "None",
            readBalanceBehaviorValue: dto.ReadBalanceBehavior || "None",
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
            LoadBalanceBehavior: formData.loadBalancerValue,
            LoadBalancerContextSeed: formData.loadBalancerSeedEnabled ? formData.loadBalancerSeedValue : null,
            ReadBalanceBehavior: formData.readBalanceBehaviorValue,
            MaxNumberOfRequestsPerSession: formData.maximumNumberOfRequestsValue,
            Disabled: !formData.overrideConfig,
            Etag: undefined,
        };
    }
}
