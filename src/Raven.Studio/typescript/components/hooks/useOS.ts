import { useMemo } from "react";

export type OperatingSystem = "Unknown" | "MacOS" | "iOS" | "Windows" | "Android" | "Linux";

export function useOS(): OperatingSystem {
    return useMemo(() => {
        if (typeof window === "undefined") {
            return "Unknown";
        }

        const { userAgent } = window.navigator;

        if (/(Win32)|(Win64)|(Windows)|(WinCE)/i.test(userAgent)) {
            return "Windows";
        }
        if (/(Macintosh)|(MacIntel)|(MacPPC)|(Mac68K)/i.test(userAgent)) {
            return "MacOS";
        }
        if (/Linux/.test(userAgent)) {
            return "Linux";
        }
        if (/Android/.test(userAgent)) {
            return "Android";
        }
        if (/(iPhone)|(iPad)|(iPod)/i.test(userAgent)) {
            return "iOS";
        }

        return "Unknown";
    }, []);
}
