using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Test;

public sealed class TestIndexParameters
{
    public IndexDefinition IndexDefinition { get; set; }

    public string Query { get; set; }
    
    public object QueryParameters { get; set; }
    
    public int? MaxDocumentsToProcess { get; set; }
    
    public int? WaitForNonStaleResultsTimeoutInSec { get; set; }
}
