const LOGIN_PATH = "/login";

export function requestedDestination(state: unknown): string | null {
    const from =
        typeof state === "object" && state !== null && "from" in state ? (state as { from: unknown }).from : null;

    if (typeof from !== "string") {
        return null;
    }

    const isPathInThisApp = from.startsWith("/") && !from.startsWith("//") && !from.startsWith("/\\");
    const isLogin = from === LOGIN_PATH || from.startsWith(`${LOGIN_PATH}?`) || from.startsWith(`${LOGIN_PATH}#`);

    return isPathInThisApp && !isLogin ? from : null;
}
