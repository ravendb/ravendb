import assertUnreachable from "components/utils/assertUnreachable";

export function getAccessRequiredMessage(requiredAccessLevel: accessLevel) {
    const accessLevelText = getAccessLevelDisplayName(requiredAccessLevel);
    return `You don't have the required permissions (${accessLevelText} access required)`;
}

export function getAccessLevelDisplayName(accessLevel: accessLevel) {
    switch (accessLevel) {
        case "DatabaseAdmin":
            return "Database Admin";
        case "DatabaseReadWrite":
            return "Database Write";
        case "DatabaseRead":
            return "Database Read";
        case "ClusterAdmin":
            return "Cluster Admin";
        case "ClusterNode":
            return "Cluster Node";
        case "Operator":
            return "Operator";
        case "ValidUser":
            return "Valid User";
        case "UnauthenticatedClients":
            return "Unauthenticated Clients";
        default:
            assertUnreachable(accessLevel);
    }
}
