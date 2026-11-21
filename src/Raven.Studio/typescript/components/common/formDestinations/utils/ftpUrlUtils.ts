export type FtpProtocol = "ftp" | "ftps";

export function getFtpProtocol(url: string): FtpProtocol | null {
    const normalizedUrl = url?.trim().toLowerCase() ?? "";

    if (normalizedUrl.startsWith("ftps://")) {
        return "ftps";
    }
    if (normalizedUrl.startsWith("ftp://")) {
        return "ftp";
    }
    return null;
}

export function isSupportedFtpUrl(url: string): boolean {
    const trimmedUrl = url?.trim() ?? "";

    if (!trimmedUrl) {
        return false;
    }
    return !trimmedUrl.includes("://") || getFtpProtocol(trimmedUrl) != null;
}
