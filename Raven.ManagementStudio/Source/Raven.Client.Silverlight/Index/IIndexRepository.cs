namespace Raven.Client.Silverlight.Index
{
    using System.Collections.Generic;
    using Raven.Client.Silverlight.Common;

    public interface IIndexRepository
    {
        void Get<T>(string name, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback) where T : Data.Index;

        void GetMany<T>(string[] names, string[] existingNames, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : Data.Index;

        void Put<T>(T entity, CallbackFunction.Save callback, CallbackFunction.Store<T> storeCallback) where T : Data.Index;

        void Delete<T>(T entity, CallbackFunction.Save callback) where T : Data.Index;
    }
}
