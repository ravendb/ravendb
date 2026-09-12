import { CertificateItem } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import moment from "moment";

const { getState } = certificatesUtils;

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
});
