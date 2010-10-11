using Raven.Database.Data;
using Raven.Database.Storage;

namespace Raven.Database.LinearQueries
{
    public interface IRemoteSingleQueryRunner
    {
        RemoteQueryResults Query(LinearQuery query);
    }
}