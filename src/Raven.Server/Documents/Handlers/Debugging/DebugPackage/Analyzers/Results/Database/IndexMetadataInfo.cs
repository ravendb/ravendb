using System.Collections.Generic;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

public class IndexMetadataInfo
{
        public string Name { get; set; }
        
        public long Version { get; set; }
        
        public string Type { get; set; }
        
        public IndexState State { get; set; }
        
        public IndexLockMode LockMode { get; set; }
        
        public string SourceType { get; set; }
        
        public IndexPriority Priority { get; set; }
        
        public SearchEngineType SearchEngineType { get; set; }
        
        public bool HasDynamicFields { get; set; }
        
        public bool HasCompareExchange { get; set; }
        
        public bool HasTimeFields { get; set; }
        
        public List<string> TimeFields { get; set; } = new List<string>();
}
