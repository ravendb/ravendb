using System;
using Raven.Database.Data;
using Raven.Database.Storage;

namespace Raven.Database.LinearQueries
{
    public interface IRemoteSingleQueryRunner : IDisposable
    {
        RemoteQueryResults Query(LinearQuery query);
    }
}