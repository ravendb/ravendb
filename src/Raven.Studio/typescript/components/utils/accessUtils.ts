import assertUnreachable from "components/utils/assertUnreachable";

export function getDatabaseAccessRequiredMessage(requiredAccessLevel: databaseAccessLevel) {
    const accessLevelText = getAccessLevelDisplayName(requiredAccessLevel);
    return `You don't have the required permissions (${accessLevelText} access required)`;
}

function getAccessLevelDisplayName(accessLevel: databaseAccessLevel) {
    switch (accessLevel) {
        case "DatabaseAdmin":
            return "Database Admin";
        case "DatabaseReadWrite":
            return "Database Write";
        case "DatabaseRead":
            return "Database Read";
        default:
            assertUnreachable(accessLevel);
    }
}
