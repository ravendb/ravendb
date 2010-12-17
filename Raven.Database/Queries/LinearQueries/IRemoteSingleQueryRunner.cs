using System;
using Raven.Database.Data;

namespace Raven.Database.Queries.LinearQueries
{
    public interface IRemoteSingleQueryRunner : IDisposable
    {
        RemoteQueryResults Query(LinearQuery query);
    }
}