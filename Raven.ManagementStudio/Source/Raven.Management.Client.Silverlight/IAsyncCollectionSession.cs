namespace Raven.Management.Client.Silverlight
{
    using System.Collections.Generic;
    using Raven.Database;
    using Raven.Management.Client.Silverlight.Common;

    public interface IAsyncCollectionSession
    {
        void Load(string collectionName, CallbackFunction.Load<IList<JsonDocument>> callback);
    }
}