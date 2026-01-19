export type DatabaseAccessLevel = "DatabaseAdmin" | "DatabaseReadWrite" | "DatabaseRead";

export function getDatabaseAccessRequiredMessage(requiredAccessLevel: DatabaseAccessLevel) {
    const accessLevelText = getAccessLevelDisplayName(requiredAccessLevel);
    return `You don't have the required permissions (${accessLevelText} access required)`;
}

function getAccessLevelDisplayName(accessLevel: DatabaseAccessLevel) {
    switch (accessLevel) {
        case "DatabaseAdmin":
            return "Database Admin";
        case "DatabaseReadWrite":
            return "Database Write";
        case "DatabaseRead":
            return "Database Read";
        default:
            return accessLevel;
    }
}
