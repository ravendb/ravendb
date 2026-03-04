import { SetupWizardFormData } from "components/setupWizard/setupWizardValidation";

export function getLicenseType(licenseInfo: SetupWizardFormData["licenseKeyStep"]["licenseInfo"]) {
    const type = licenseInfo?.licenseType || "None";

    const licenseTiers: Record<Raven.Server.Commercial.LicenseType, number> = {
        None: 0,
        Invalid: 0,
        Reserved: 0,
        Community: 1,
        Essential: 1,
        Professional: 3,
        Enterprise: 4,
        EnterpriseAi: 4,
        Developer: 4, // Enterprise-level set of features
    };

    return {
        type,
        isAtLeast: (minimumType: Raven.Server.Commercial.LicenseType) =>
            (licenseTiers[type] || 0) >= (licenseTiers[minimumType] || 0),
        isHigherThan: (compareType: Raven.Server.Commercial.LicenseType) =>
            (licenseTiers[type] || 0) > (licenseTiers[compareType] || 0),
        isCommunity: () => type === "Community" || type === "Essential", // I think community and essential are the same
        isDeveloper: () => type === "Developer",
        isProfessionalOrHigher: () => (licenseTiers[type] || 0) >= licenseTiers["Professional"],
        isEnterprise: () => type === "Enterprise",
        hasLicense: () => type !== "None" && type !== "Invalid",
    };
}

export function fileToBase64(file: File): Promise<string> {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.readAsDataURL(file);
        reader.onload = () => resolve(reader.result as string);
        reader.onerror = reject;
    });
}

export function base64ToFile(base64: string, filename: string, mimeType = "application/x-pkcs12"): File {
    const byteString = atob(base64);
    const arrayBuffer = new ArrayBuffer(byteString.length);
    const intArray = new Uint8Array(arrayBuffer);

    for (let i = 0; i < byteString.length; i++) {
        intArray[i] = byteString.charCodeAt(i);
    }

    return new File([intArray], filename, { type: mimeType });
}

export function getFullDomain(domainStep: SetupWizardFormData["domainStep"]) {
    return domainStep.domain + "." + domainStep.rootDomain;
}

export function sanitizeCommonName(cn: string) {
    return cn.replace(/\*/g, "").replace(/\.{2,}/g, ".");
}
