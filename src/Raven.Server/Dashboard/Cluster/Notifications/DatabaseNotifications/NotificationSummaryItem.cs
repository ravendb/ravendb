using System.Text.RegularExpressions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications;

public class NotificationSummaryItem
{
    public string Reason { get; set; }
    public string PrettifiedReason { get; set; }
    public long Count { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Reason)] = Reason,
            [nameof(PrettifiedReason)] = PrettifiedReason,
            [nameof(Count)] = Count
        };
    }
    
    public static string PrettifyReason(string reason)
    {
        var parts = reason.Split(['_'], 2);
        
        if (parts.Length < 2)
        {
            return AddSpacesToPascalCase(reason);
        }
        
        var firstPart = parts[0];
        var secondPart = parts[1];
        
        var formattedSecondPart = AddSpacesToPascalCase(secondPart);
        
        return $"{firstPart}: {formattedSecondPart}";
    }
    
    private static string AddSpacesToPascalCase(string text)
    {
        return Regex.Replace(
            text,
            "(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])",
            " ",
            RegexOptions.Compiled
        );
    }
}
