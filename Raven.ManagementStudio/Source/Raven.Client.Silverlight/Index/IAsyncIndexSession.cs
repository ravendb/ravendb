namespace Raven.Client.Silverlight.Index
{
    using System.Collections.Generic;
    using Raven.Client.Silverlight.Common;

    public interface IAsyncIndexSession
    {
        void Load<T>(string name, CallbackFunction.Load<T> callback) where T : Data.Index;

        void LoadMany<T>(CallbackFunction.Load<IList<T>> callback) where T : Data.Index;

        void StoreEntity<T>(T entity) where T : Data.Index;

        void Delete<T>(T entity) where T : Data.Index;

        void SaveChanges(CallbackFunction.Save callback);
    }
}
