import React from "react";
import ConnectionStringError from "./ConnectionTestError";
import RichAlert from "components/common/RichAlert";

interface ConnectionTestResultProps {
    testResult: Raven.Server.Web.System.NodeConnectionTestResult;
}

export default function ConnectionTestResult({ testResult }: ConnectionTestResultProps) {
    if (!testResult) {
        return null;
    }

    return testResult.Success ? (
        <RichAlert variant="success">Successfully connected</RichAlert>
    ) : (
        <ConnectionStringError message={testResult.Error} />
    );
}
