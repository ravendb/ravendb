using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene
{
    public interface IIndexReadActions : IDisposable
    {
        IEnumerable<string> Query(IndexQuery query, CancellationToken token);
    }
}