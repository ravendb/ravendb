namespace Raven.Client.Silverlight.Index
{
    using System.Collections.Generic;
    using Raven.Client.Silverlight.Common;
    using Raven.Client.Silverlight.Data;

    public interface IAsyncIndexSession
    {
        void Load<T>(string name, CallbackFunction.Load<T> callback) where T : JsonIndex;

        void LoadMany<T>(CallbackFunction.Load<IList<T>> callback) where T : JsonIndex;

        void StoreEntity<T>(T entity) where T : JsonIndex;

        void Delete<T>(T entity) where T : JsonIndex;

        void SaveChanges(CallbackFunction.Save<JsonIndex> callback);

        void Refresh<T>(T entity, CallbackFunction.Load<T> callback) where T : JsonIndex;
    }
}
