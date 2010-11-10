namespace Raven.Client.Silverlight.Document
{
    using System.Collections.Generic;
    using Raven.Client.Silverlight.Common;
    using Raven.Client.Silverlight.Data;

    public interface IDocumentRepository
    {
        void Get<T>(string key, CallbackFunction.Load<T> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument;

        void GetMany<T>(string[] keys, IList<T> existingEntities, CallbackFunction.Load<IList<T>> callback, CallbackFunction.Store<IList<T>> storeCallback) where T : JsonDocument;

        void Put<T>(T entity, CallbackFunction.Save<IList<T>> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument;

        void Post<T>(T entity, CallbackFunction.Save<IList<T>> callback, CallbackFunction.Store<T> storeCallback) where T : JsonDocument;

        void Delete<T>(T entity, CallbackFunction.Save<IList<T>> callback) where T : JsonDocument;

        void Batch<T>(IList<BatchCommand<T>> batchCommands, CallbackFunction.Save<IList<T>> callback) where T : JsonDocument;
    }
}