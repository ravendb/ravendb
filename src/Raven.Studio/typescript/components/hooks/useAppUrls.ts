import appUrl from "common/appUrl";

export function useAppUrls() {
    return appUrl.forCurrentDatabase();
}
