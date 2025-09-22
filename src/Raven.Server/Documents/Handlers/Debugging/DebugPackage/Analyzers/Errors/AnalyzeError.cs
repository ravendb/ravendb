using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;

public class AnalyzeError(string componentName, string errorMessage, AnalyzeErrorSeverity severity) : IDynamicJson
{
    public string ComponentName { get; } = componentName;
    
    public string ErrorMessage { get; } = errorMessage;

    public AnalyzeErrorSeverity Severity { get; } = severity;

    public string Exception { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ComponentName)] = ComponentName,
            [nameof(ErrorMessage)] = ErrorMessage,
            [nameof(Severity)] = Severity.ToString(),
            [nameof(Exception)] = Exception
        };
    }
}
