using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details;

public sealed class CpuCreditsExhaustionWarning : INotificationDetails
{
    public CpuCreditsExhaustionWarning()
    {
        // deserialization
    }
    
    public HashSet<string> IndexNames { get; set; }
    
    public CpuCreditsExhaustionWarning(HashSet<string> indexNames)
    {
        IndexNames = indexNames;
    }
    
    public DynamicJsonValue ToJson()
    {
        var djv = new DynamicJsonValue(GetType());
        
        var indexNames = new DynamicJsonArray();

        foreach (var indexName in IndexNames)
        {
            indexNames.Add(indexName);
        }
        
        djv[nameof(IndexNames)] = indexNames;

        return djv;
    }
}
