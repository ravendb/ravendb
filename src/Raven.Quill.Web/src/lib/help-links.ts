import { Activity, BookOpen, LifeBuoy, Users, type LucideIcon } from "lucide-react";

const SUPPORT_URL = "https://ravendb.net/support";

// Support carries the license id so the request arrives already identified. The id is
// missing until the license query resolves, which just means a plain support link.
export function getHelpLinks(licenseId: string | undefined): { label: string; href: string; icon: LucideIcon }[] {
    return [
        { label: "Support", href: getSupportUrl(licenseId), icon: LifeBuoy },
        { label: "Documentation", href: "https://docs.ravendb.net/quill", icon: BookOpen },
        { label: "Community", href: "https://ravendb.net/community", icon: Users },
        { label: "Service status", href: "https://status.ravendb.net", icon: Activity },
    ];
}

function getSupportUrl(licenseId: string | undefined) {
    if (!licenseId) {
        return SUPPORT_URL;
    }

    return `${SUPPORT_URL}?${new URLSearchParams({ licenseId })}`;
}
