export const DISCORD_DEVELOPER_PORTAL_URL = "https://discord.com/developers/applications";

const DM_ONLY_PERMISSIONS = "0";

export function discordInstallUrl(applicationId: string) {
    const params = new URLSearchParams({
        client_id: applicationId,
        scope: "bot",
        permissions: DM_ONLY_PERMISSIONS,
    });

    return `https://discord.com/oauth2/authorize?${params.toString()}`;
}
