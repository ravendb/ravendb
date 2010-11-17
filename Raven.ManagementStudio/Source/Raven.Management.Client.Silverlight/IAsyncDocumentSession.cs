namespace Raven.Management.Client.Silverlight
{
    using System;
    using System.Collections.Generic;
    using Common;

    /// <summary>
    /// Interface for document session using async approaches
    /// </summary>
    public interface IAsyncDocumentSession : IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="callback"></param>
        void Load<T>(string key, CallbackFunction.Load<T> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="callback"></param>
        void LoadMany<T>(string[] keys, CallbackFunction.Load<IList<T>> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        void LoadMany<T>(CallbackFunction.Load<IList<T>> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        void Store(object entity);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        void Delete<T>(T entity);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        void SaveChanges(CallbackFunction.Save<object> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="callback"></param>
        void Refresh<T>(T entity, CallbackFunction.Load<T> callback);
    }
}