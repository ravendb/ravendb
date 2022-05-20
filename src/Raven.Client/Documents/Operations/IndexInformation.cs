using System;
using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Operations;

public class IndexInformation : BasicIndexInformation
{
    public bool IsStale { get; set; }

    public IndexState State { get; set; }

    public DateTime? LastIndexingTime { get; set; }
}
