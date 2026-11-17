import ClientConfigurationUtils from "./ClientConfigurationUtils";
import { ClientConfigurationFormData } from "./ClientConfigurationValidation";

const defaultFormData: ClientConfigurationFormData = {
    overrideConfig: true,
    identityPartsSeparatorValue: null,
    maximumNumberOfRequestsValue: null,
    loadBalancerValue: null,
    loadBalancerSeedValue: null,
    readBalanceBehaviorValue: null,
};

describe("ClientConfigurationUtils", () => {
    it("saves Default as null and loads it back as Default", () => {
        const dto = ClientConfigurationUtils.mapToDto(defaultFormData, false);

        expect(dto).toMatchObject({
            IdentityPartsSeparator: null,
            LoadBalanceBehavior: null,
            LoadBalancerContextSeed: null,
            ReadBalanceBehavior: null,
            MaxNumberOfRequestsPerSession: null,
            Disabled: false,
        });
        expect(ClientConfigurationUtils.mapToFormData(dto)).toEqual(defaultFormData);
    });

    it("saves None as None and loads it back as None", () => {
        const noneFormData: ClientConfigurationFormData = {
            ...defaultFormData,
            loadBalancerValue: "None",
            readBalanceBehaviorValue: "None",
        };

        const dto = ClientConfigurationUtils.mapToDto(noneFormData, false);

        expect(dto.LoadBalanceBehavior).toBe("None");
        expect(dto.ReadBalanceBehavior).toBe("None");
        expect(ClientConfigurationUtils.mapToFormData(dto)).toEqual(noneFormData);
    });

    it("saves explicit values and loads them back unchanged", () => {
        const explicitFormData: ClientConfigurationFormData = {
            overrideConfig: true,
            identityPartsSeparatorValue: "-",
            maximumNumberOfRequestsValue: 50,
            loadBalancerValue: "UseSessionContext",
            loadBalancerSeedValue: 7,
            readBalanceBehaviorValue: "FastestNode",
        };

        const dto = ClientConfigurationUtils.mapToDto(explicitFormData, false);

        expect(dto).toMatchObject({
            IdentityPartsSeparator: "-",
            MaxNumberOfRequestsPerSession: 50,
            LoadBalanceBehavior: "UseSessionContext",
            LoadBalancerContextSeed: 7,
            ReadBalanceBehavior: "FastestNode",
        });
        expect(ClientConfigurationUtils.mapToFormData(dto)).toEqual(explicitFormData);
    });

    it("saves a disabled database override as Disabled only", () => {
        const dto = ClientConfigurationUtils.mapToDto({ ...defaultFormData, overrideConfig: false }, false);

        expect(dto).toEqual({ Disabled: true, Etag: undefined });
    });
});
