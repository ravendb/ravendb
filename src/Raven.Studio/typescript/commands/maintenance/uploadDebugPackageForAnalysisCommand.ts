import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

class uploadDebugPackageForAnalysisCommand extends commandBase {

    constructor(private file: File) {
        super();
    }

    execute(): JQueryPromise<DebugPackageAnalysisSummary> {
        const url = endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerUpload;

        // the server reads the raw .zip from the request body (not multipart), so send the File as-is
        const options: JQueryAjaxSettings = {
            processData: false,
            contentType: false,
            cache: false,
        };

        return this.post<DebugPackageAnalysisSummary>(url, this.file, null, options, 60000)
            .fail((response: JQueryXHR) =>
                this.reportError("Failed to analyze the debug package", response.responseText, response.statusText));
    }
}

export = uploadDebugPackageForAnalysisCommand;
