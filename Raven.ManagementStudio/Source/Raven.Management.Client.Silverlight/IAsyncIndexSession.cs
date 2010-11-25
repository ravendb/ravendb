namespace Raven.Management.Client.Silverlight
{
    using System.Collections.Generic;
    using Raven.Database.Data;
    using Raven.Database.Indexing;
    using Raven.Management.Client.Silverlight.Common;

    public interface IAsyncIndexSession
    {
        void Query(string index, IndexQuery query, string[] includes, CallbackFunction.Load<QueryResult> callback);

        void LoadMany(CallbackFunction.Load<IDictionary<string, IndexDefinition>>  callback);

        void Save(KeyValuePair<string, IndexDefinition> index, CallbackFunction.SaveOne<KeyValuePair<string, IndexDefinition>> callback);

        void Delete(string name, CallbackFunction.SaveOne<string> callback);
    }
}