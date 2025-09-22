using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;

public class DebugPackageAnalyzeErrors : IDynamicJson
{
    public List<AnalyzeError> Errors { get; set; } = [];
    
    public void AddFileError(string entryFullName, string readToEnd)
    {
        // TODO arek
    }
    
    public void AddAnalyzerError(string analyzerName, string analyzeFailureMessage, AnalyzeErrorSeverity severity, Exception e = null)
    {
        Errors.Add(new AnalyzeError(analyzerName, analyzeFailureMessage, severity)
        {
            Exception = e?.ToString()
        });
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Errors)] = new DynamicJsonArray(Errors?.Select(error => error.ToJson()))
        };
    }
}
