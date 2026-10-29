import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificateItem } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import moment from "moment";

const { getState, isNew2FAKeyNeeded, is2FARemoved, mapEditToDto } = certificatesUtils;

function createCertificate(overrides: Partial<CertificateItem> = {}): CertificateItem {
    return {
        Name: "Some cert",
        Thumbprint: "0F61904E1926ED2EDD5BB4BA8BC34742960B7839",
        SecurityClearance: "ValidUser",
        Permissions: {},
        NotAfter: moment()
            .add(2 as const, "years")
            .format(),
        NotBefore: moment()
            .subtract(10 as const, "days")
            .format(),
        ...overrides,
    };
}

describe("certificatesUtils", () => {
    describe("getState", () => {
        it("should return Valid for a certificate that expires far in the future", () => {
            expect(getState(createCertificate())).toBe("Valid");
        });

        it("should return About to expire for a certificate that expires within the threshold", () => {
            const cert = createCertificate({
                NotAfter: moment()
                    .add(5 as const, "days")
                    .format(),
            });

            expect(getState(cert)).toBe("About to expire");
        });

        it("should return Expired for a certificate past its expiration date", () => {
            const cert = createCertificate({
                NotAfter: moment()
                    .subtract(5 as const, "days")
                    .format(),
            });

            expect(getState(cert)).toBe("Expired");
        });

        it("should return Disabled for a disabled certificate", () => {
            expect(getState(createCertificate({ Disabled: true }))).toBe("Disabled");
        });

        it("should return Disabled for a disabled certificate that is also expired", () => {
            const cert = createCertificate({
                Disabled: true,
                NotAfter: moment()
                    .subtract(5 as const, "days")
                    .format(),
            });

            expect(getState(cert)).toBe("Disabled");
        });

        it("should return Valid for an SSO user entry which has no expiration date", () => {
            expect(getState(createCertificate({ NotAfter: null }))).toBe("Valid");
        });

        it("should return Disabled for a disabled SSO user entry which has no expiration date", () => {
            expect(getState(createCertificate({ NotAfter: null, Disabled: true }))).toBe("Disabled");
        });
    });

    describe("isNew2FAKeyNeeded", () => {
        it("should need a key when 2FA is turned on for a certificate without it", () => {
            expect(isNew2FAKeyNeeded({ isRequire2FA: true, isRegenerate2FAKey: false }, false)).toBe(true);
        });

        it("should not need a key when the certificate keeps its current key", () => {
            expect(isNew2FAKeyNeeded({ isRequire2FA: true, isRegenerate2FAKey: false }, true)).toBe(false);
        });

        it("should need a key when a new one is requested for a certificate with 2FA", () => {
            expect(isNew2FAKeyNeeded({ isRequire2FA: true, isRegenerate2FAKey: true }, true)).toBe(true);
        });

        it("should not need a key when 2FA is off", () => {
            expect(isNew2FAKeyNeeded({ isRequire2FA: false, isRegenerate2FAKey: true }, true)).toBe(false);
        });
    });

    describe("is2FARemoved", () => {
        it("should remove 2FA when it is turned off for a certificate with 2FA", () => {
            expect(is2FARemoved({ isRequire2FA: false }, true)).toBe(true);
        });

        it("should not remove 2FA when it stays on", () => {
            expect(is2FARemoved({ isRequire2FA: true }, true)).toBe(false);
        });

        it("should not remove 2FA when the certificate has none", () => {
            expect(is2FARemoved({ isRequire2FA: false }, false)).toBe(false);
        });
    });

    describe("mapEditToDto", () => {
        const formData: CertificatesEditFormData = {
            securityClearance: "ValidUser",
            databasePermissions: [],
            isRequire2FA: true,
            isRegenerate2FAKey: false,
            authenticationKey: "RNHRX6WXCLZVQPJSW5NSWV64JU65E5WA",
        };

        it("should not send a 2FA key when the certificate keeps its current key", () => {
            const dto = mapEditToDto(formData, createCertificate({ HasTwoFactor: true }));

            expect(dto.TwoFactorAuthenticationKey).toBeNull();
        });

        it("should send the new 2FA key when one is requested", () => {
            const dto = mapEditToDto(
                { ...formData, isRegenerate2FAKey: true },
                createCertificate({ HasTwoFactor: true })
            );

            expect(dto.TwoFactorAuthenticationKey).toBe(formData.authenticationKey);
        });
    });
});
