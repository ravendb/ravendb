using System.Collections.Generic;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Management.Client.Silverlight.Common;

namespace Raven.Management.Client.Silverlight
{

    public interface IAsyncIndexSession
    {
        void Query(string index, IndexQuery query, string[] includes, CallbackFunction.Load<QueryResult> callback);

        void LinearQuery(string query, int start, int pageSize, CallbackFunction.Load<IList<JsonDocument>> callback);

        void LoadMany(CallbackFunction.Load<IDictionary<string, IndexDefinition>>  callback);

        void Save(string name, IndexDefinition definition, CallbackFunction.SaveOne<KeyValuePair<string, IndexDefinition>> callback);

        void Delete(string name, CallbackFunction.SaveOne<string> callback);
    }
}