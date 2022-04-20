import appUrl from "common/appUrl";

export function useAppUrls() {
    return {
        forCurrentDatabase: appUrl.forCurrentDatabase(),
        appUrl,
    };
}
