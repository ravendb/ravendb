import React from "react";
import { Alert } from "reactstrap";
import ConnectionStringError from "./ConnectionStringError";

interface ConnectionStringTestResultProps {
    testResult: Raven.Server.Web.System.NodeConnectionTestResult;
}

export default function ConnectionStringTestResult({ testResult }: ConnectionStringTestResultProps) {
    if (!testResult) {
        return null;
    }

    return testResult.Success ? (
        <Alert color="success">Successfully connected</Alert>
    ) : (
        <ConnectionStringError message={testResult.Error} />
    );
}
