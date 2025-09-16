using System.Collections.Generic;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageContent
{
    private Dictionary<string, DebugPackageEntries> _databaseEntries = new();
    
    public DebugPackageEntries ServerEntries { get; } = new();

    public HashSet<string> DatabaseNames { get; private set; } = new();
    
    public DebugPackageEntries ForDatabase(string databaseName)
    {
        if (_databaseEntries.TryGetValue(databaseName, out var entries) == false)
        {
            entries = new DebugPackageEntries();
            _databaseEntries.Add(databaseName, entries);
            
            DatabaseNames.Add(databaseName);
        }
        
        return entries;
    }
}
