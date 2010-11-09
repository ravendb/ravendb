namespace Raven.Client.Silverlight.Index
{
    using System.Collections.Generic;
    using Raven.Client.Silverlight.Common;
    using Raven.Client.Silverlight.Data;

    public interface IIndexRepository
    {
        void Get<T>(string name, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonIndex;

        void GetMany<T>(string[] names, IList<T> existingEntities, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : JsonIndex;

        void Put<T>(T entity, CallbackFunction.Save<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonIndex;

        void Delete<T>(T entity, CallbackFunction.Save<T> callback) where T : JsonIndex;
    }
}
