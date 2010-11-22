namespace Raven.Management.Client.Silverlight
{
    using Raven.Database.Data;
    using Raven.Management.Client.Silverlight.Common;

    public interface IAsyncIndexSession
    {
        void Query(string index, IndexQuery query, string[] includes, CallbackFunction.Load<QueryResult> callback);
    }
}