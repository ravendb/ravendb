import { formatDryRunError } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskVerificationAlert";

describe("formatDryRunError", () => {
    it("keeps every message line before the inner exception in the message", () => {
        const error = [
            "System.InvalidOperationException: Insufficient permissions to enable CDC on the database.",
            "SQL Server error: CDC is not supported in Express edition.",
            "",
            "An administrator can enable it manually by running the following script:",
            "",
            "EXEC sys.sp_cdc_enable_db;",
            " ---> Microsoft.Data.SqlClient.SqlException (0x80131904): CDC is not supported in Express edition.",
            "   at Raven.Server.Documents.CdcSink.SqlServerCdcSinkProcess.EnableCdcAsync()",
            "   --- End of inner exception stack trace ---",
            "   at Raven.Server.Documents.CdcSink.Test.CdcSinkTestProcess.RunAsync()",
        ].join("\r\n");

        expect(formatDryRunError(error)).toEqual({
            message: [
                "Insufficient permissions to enable CDC on the database.",
                "SQL Server error: CDC is not supported in Express edition.",
                "",
                "An administrator can enable it manually by running the following script:",
                "",
                "EXEC sys.sp_cdc_enable_db;",
            ].join("\n"),
            details: [
                "---> Microsoft.Data.SqlClient.SqlException (0x80131904): CDC is not supported in Express edition.",
                "   at Raven.Server.Documents.CdcSink.SqlServerCdcSinkProcess.EnableCdcAsync()",
                "   --- End of inner exception stack trace ---",
                "   at Raven.Server.Documents.CdcSink.Test.CdcSinkTestProcess.RunAsync()",
            ].join("\n"),
        });
    });

    it("keeps all joined validation errors in the message", () => {
        const error = "System.InvalidOperationException: First error\nSecond error";

        expect(formatDryRunError(error)).toEqual({ message: "First error\nSecond error" });
    });

    it("strips an exception type prefix with an HResult", () => {
        const error = [
            "Npgsql.NpgsqlException (0x80004005): Failed to connect to 127.0.0.1:5432",
            "   at Npgsql.Internal.NpgsqlConnector.Connect()",
        ].join("\n");

        expect(formatDryRunError(error)).toEqual({
            message: "Failed to connect to 127.0.0.1:5432",
            details: "at Npgsql.Internal.NpgsqlConnector.Connect()",
        });
    });

    it("falls back to a generic message when the error is empty", () => {
        expect(formatDryRunError(null)).toEqual({ message: "The CDC dry run failed." });
    });
});
