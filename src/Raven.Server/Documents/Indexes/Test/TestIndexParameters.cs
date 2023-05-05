using Raven.Client.Documents.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Test;

public class TestIndexParameters
{
    public IndexDefinition IndexDefinition { get; set; }

    public string Query { get; set; }
    
    public object QueryParameters { get; set; }
    
    public int? MaxDocumentsToProcess { get; set; }
    
    public int? WaitForNonStaleResultsTimeout { get; set; }
}
