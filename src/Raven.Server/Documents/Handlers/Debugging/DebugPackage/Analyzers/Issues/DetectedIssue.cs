using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;

public class DetectedIssue : IDynamicJson
{
    public DetectedIssue()
    {
        // deserialization
    }
    
    public DetectedIssue(string title, string description, IssueSeverity severity, IssueCategory category)
    {
        Title = title;
        Description = description;
        Severity = severity;
        Category = category;
    }

    public string Title { get; set; }

    public string Description { get; set; }

    public IssueSeverity Severity { get; set; }
    
    public IssueCategory Category { get; set; }
    
    public string RecommendedAction { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Title)] = Title,
            [nameof(Description)] = Description,
            [nameof(Severity)] = Severity.ToString(),
            [nameof(Category)] = Category.ToString(),
            [nameof(RecommendedAction)] = RecommendedAction
        };
    }
}
