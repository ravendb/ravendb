using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations.Queries;

public class RoundRobinComparer : IComparer<BlittableJsonReaderObject>
{
    private long _current;

    public int Compare(BlittableJsonReaderObject _, BlittableJsonReaderObject __)
    {
        return _current++ % 2 == 0 ? 1 : -1;
    }
}
