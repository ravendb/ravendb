using Raven.Client.Documents.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Test;

public class TestIndexParameters
{
    public IndexDefinition IndexDefinition { get; set; }

    public BlittableJsonReaderObject Query { get; set; }
}
