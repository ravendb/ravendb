import { ClientConfigurationFormData } from "./ClientConfigurationValidation";
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior;
import LoadBalanceBehavior = Raven.Client.Http.LoadBalanceBehavior;
import assertUnreachable from "components/utils/assertUnreachable";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "../select/Select";

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

    static getReadBalanceBehaviorOptions(): SelectOption<ReadBalanceBehavior>[] {
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

    static getLoadBalanceBehaviorOptions(): SelectOption<LoadBalanceBehavior>[] {
        return ClientConfigurationUtils.allLoadBalanceBehaviors.map((value) => ({
            value,
            label: ClientConfigurationUtils.formatLoadBalanceBehavior(value),
        }));
    }

    static mapToFormData(dto: ClientConfiguration, overrideConfig = true): ClientConfigurationFormData {
        return {
            overrideConfig,
            identityPartsSeparatorValue: dto?.IdentityPartsSeparator || null,
            maximumNumberOfRequestsValue: dto?.MaxNumberOfRequestsPerSession || null,
            loadBalancerValue: dto?.LoadBalanceBehavior || null,
            loadBalancerSeedValue: dto?.LoadBalancerContextSeed || null,
            readBalanceBehaviorValue: dto?.ReadBalanceBehavior || null,
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
            LoadBalancerContextSeed: formData.loadBalancerSeedValue,
            ReadBalanceBehavior: formData.readBalanceBehaviorValue,
            MaxNumberOfRequestsPerSession: formData.maximumNumberOfRequestsValue,
            Disabled: !formData.overrideConfig,
            Etag: undefined,
        };
    }
}
