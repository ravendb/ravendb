import React from "react";
import { Alert } from "reactstrap";
import ConnectionStringError from "./ConnectionTestError";

interface ConnectionTestResultProps {
    testResult: Raven.Server.Web.System.NodeConnectionTestResult;
}

export default function ConnectionTestResult({ testResult }: ConnectionTestResultProps) {
    if (!testResult) {
        return null;
    }

    return testResult.Success ? (
        <Alert color="success">Successfully connected</Alert>
    ) : (
        <ConnectionStringError message={testResult.Error} />
    );
}
