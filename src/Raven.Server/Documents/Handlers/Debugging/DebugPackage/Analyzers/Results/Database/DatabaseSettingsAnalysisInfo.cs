using System.Collections.Generic;
using Raven.Server.Config;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

public class DatabaseSettingsAnalysisInfo
{
    public Dictionary<string, ConfigurationEntrySingleValue> Settings { get; set; } = new();
    
    public DebugPackageEntries.Entry SettingsEntry { get; set; } 
}
