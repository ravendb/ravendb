namespace Raven.Client.Silverlight.Document
{
    using System;
    using System.Collections.Generic;
    using Raven.Client.Silverlight.Common;
    using Raven.Client.Silverlight.Data;

    public interface IAsyncDocumentSession : IDisposable
    {
        void Load<T>(string key, CallbackFunction.Load<T> callback) where T : JsonDocument;

        void LoadMany<T>(string[] keys, CallbackFunction.Load<IList<T>> callback) where T : JsonDocument;

        void LoadMany<T>(CallbackFunction.Load<IList<T>> callback) where T : JsonDocument;

        void StoreEntity<T>(T entity) where T : JsonDocument;

        void Delete<T>(T entity) where T : JsonDocument;

        void SaveChanges(CallbackFunction.Save<JsonDocument> callback);
    }
}
