namespace Raven.Management.Client.Silverlight.Document
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using Client;
    using Common;
    using Database;
    using Raven.Client.Document;

    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSession
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncDocumentSession"/> class.
        /// </summary>
        /// <param name="documentStore">The document store.</param>
        public AsyncDocumentSession(DocumentStore documentStore)
            : base(documentStore)
        {
            AsyncDatabaseCommands = documentStore.AsyncDatabaseCommands;
        }

        /// <summary>
        /// Gets the async database commands.
        /// </summary>
        /// <value>The async database commands.</value>
        public IAsyncDatabaseCommands AsyncDatabaseCommands { get; private set; }

        #region IAsyncDocumentSession Members

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="callback"></param>
        public void Load<T>(string key, CallbackFunction.Load<T> callback)
        {
            Guard.Assert(() => !string.IsNullOrEmpty(key));

            SynchronizationContext context = SynchronizationContext.Current;

            object entity;
            if (entitiesByKey.TryGetValue(key, out entity))
            {
                var result = Convert<T>(entity);

                context.Post(
                    delegate
                        {
                            callback.Invoke(new LoadResponse<T>
                                                {
                                                    Data = result,
                                                    StatusCode = HttpStatusCode.OK
                                                });
                        },
                    null);

                return;
            }

            AsyncDatabaseCommands.DocumentGet(key, (result) =>
                                                       {
                                                           T data = default(T);

                                                           if (result.IsSuccess)
                                                           {
                                                               data = base.TrackEntity<T>(result.Data);
                                                           }

                                                           context.Post(delegate
                                                                            {
                                                                                callback.Invoke(new LoadResponse<T>
                                                                                                    {
                                                                                                        Data = data,
                                                                                                        Exception = result.Exception,
                                                                                                        StatusCode = result.StatusCode
                                                                                                    });
                                                                            },
                                                                        null);
                                                       });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="keys"></param>
        /// <param name="callback"></param>
        public void LoadMany<T>(string[] keys, CallbackFunction.Load<IList<T>> callback)
        {
            Guard.Assert(() => keys != null);
            Guard.Assert(() => keys.Length > 0);

            SynchronizationContext context = SynchronizationContext.Current;

            object existingEntity;

            var existingEntities = new List<T>();

            foreach (string key in keys)
            {
                entitiesByKey.TryGetValue(key, out existingEntity);
                if (existingEntity != null)
                {
                    existingEntities.Add(Convert<T>(existingEntity));
                }
            }

            if (existingEntities.Count == keys.Length)
            {
                context.Post(
                    delegate
                        {
                            callback.Invoke(
                                new LoadResponse<IList<T>>
                                    {
                                        Data = existingEntities,
                                        StatusCode = HttpStatusCode.OK
                                    });
                        },
                    null);

                return;
            }

            AsyncDatabaseCommands.DocumentGetMany(keys, (result) =>
                                                            {
                                                                List<T> responseResult = result.IsSuccess ? result.Data.Select(jsonDocument => base.TrackEntity<T>(jsonDocument)).ToList() : null;

                                                                context.Post(delegate
                                                                                 {
                                                                                     callback.Invoke(
                                                                                         new LoadResponse<IList<T>>
                                                                                             {
                                                                                                 Data = responseResult,
                                                                                                 Exception = result.Exception,
                                                                                                 StatusCode = result.StatusCode
                                                                                             });
                                                                                 },
                                                                             null);
                                                            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="callback"></param>
        public void LoadMany<T>(CallbackFunction.Load<IList<T>> callback)
        {
            SynchronizationContext context = SynchronizationContext.Current;

            AsyncDatabaseCommands.DocumentGetMany(null, (result) =>
                                                            {
                                                                List<T> responseResult = result.IsSuccess ? result.Data.Select(jsonDocument => base.TrackEntity<T>(jsonDocument)).ToList() : null;

                                                                context.Post(delegate
                                                                                 {
                                                                                     callback.Invoke(
                                                                                         new LoadResponse<IList<T>>
                                                                                             {
                                                                                                 Data = responseResult,
                                                                                                 Exception = result.Exception,
                                                                                                 StatusCode = result.StatusCode
                                                                                             });
                                                                                 },
                                                                             null);
                                                            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        public void SaveChanges(CallbackFunction.Save<object> callback)
        {
            SaveChangesData data = PrepareForSaveChanges();
            if (data.Commands.Count == 0)
                return;

            SynchronizationContext context = SynchronizationContext.Current;

            AsyncDatabaseCommands.DocumentBatch(data.Commands.ToList(), (results) =>
                                                                            {
                                                                                UpdateBatchResults(results, data.Entities);

                                                                                var responses =
                                                                                    new List<Response<object>>();

                                                                                for (int index = 0; index < results.Count; index++)
                                                                                {
                                                                                    BatchResult batchResult = results[index];
                                                                                    Response<object> response = null;
                                                                                    switch (batchResult.Method)
                                                                                    {
                                                                                        case "GET":
                                                                                            response = new LoadResponse
                                                                                                <object>
                                                                                                           {
                                                                                                               Data = data.Entities[index],
                                                                                                               StatusCode = HttpStatusCode.OK
                                                                                                           };
                                                                                            break;
                                                                                        case "POST":
                                                                                        case "PUT":
                                                                                            response = new SaveResponse
                                                                                                <object>
                                                                                                           {
                                                                                                               Data = data.Entities[index],
                                                                                                               StatusCode = HttpStatusCode.OK
                                                                                                           };
                                                                                            break;
                                                                                        case "DELETE":
                                                                                            response = new DeleteResponse
                                                                                                <object>
                                                                                                           {
                                                                                                               Data = data.Entities[index],
                                                                                                               StatusCode = HttpStatusCode.OK,
                                                                                                           };
                                                                                            break;
                                                                                    }

                                                                                    responses.Add(response);
                                                                                }

                                                                                context.Post(
                                                                                    delegate { callback.Invoke(responses); },
                                                                                    null);
                                                                            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="callback"></param>
        public void Refresh<T>(T entity, CallbackFunction.Load<T> callback)
        {
            Guard.Assert(() => entity != null);

            SynchronizationContext context = SynchronizationContext.Current;

            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
                throw new InvalidOperationException("Cannot refresh a trasient instance");

            if (deletedEntities.Contains(entity))
            {
                deletedEntities.Remove(entity);
            }

            AsyncDatabaseCommands.DocumentGet(value.Key, (result) =>
                                                             {
                                                                 T data = default(T);

                                                                 if (result.IsSuccess)
                                                                 {
                                                                     data = base.TrackEntity<T>(result.Data);
                                                                 }

                                                                 context.Post(delegate
                                                                                  {
                                                                                      callback.Invoke(new LoadResponse<T>
                                                                                                          {
                                                                                                              Data = data,
                                                                                                              Exception = result.Exception,
                                                                                                              StatusCode = result.StatusCode
                                                                                                          });
                                                                                  },
                                                                              null);
                                                             });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        public new void Delete<T>(T entity)
        {
            base.Delete(entity);
        }

        #endregion
    }
}