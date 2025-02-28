import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";
import Badge from "react-bootstrap/Badge";

interface DatabaseEnvironmentBadgeProps {
    environment: Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
}

export default function DatabaseEnvironmentBadge({ environment }: DatabaseEnvironmentBadgeProps) {
    if (!environment || environment === "None") {
        return null;
    }

    const getColor = () => {
        switch (environment) {
            case "Production":
                return "danger";
            case "Testing":
                return "success";
            case "Development":
                return "info";
            default:
                return assertUnreachable(environment);
        }
    };

    const getText = () => {
        switch (environment) {
            case "Production":
                return "Prod";
            case "Testing":
                return "Test";
            case "Development":
                return "Dev";
            default:
                return assertUnreachable(environment);
        }
    };

    return (
        <Badge className="ms-2 text-uppercase text-black" bg={getColor()} pill>
            {getText()}
        </Badge>
    );
}
