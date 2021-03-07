using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session.Loaders;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    /// <summary>
    ///     Specify interface for lazy operation for the session
    /// </summary>
    public interface ILazySessionOperations
    {
        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        ILazyLoaderWithInclude<object> Include(string path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        ILazyLoaderWithInclude<TResult> Include<TResult>(Expression<Func<TResult, string>> path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        ILazyLoaderWithInclude<TResult> Include<TResult>(Expression<Func<TResult, IEnumerable<string>>> path);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Dictionary<string, TResult>> Load<TResult>(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids and a function to call when it is evaluated
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        Lazy<Dictionary<string, TResult>> Load<TResult>(IEnumerable<string> ids, Action<Dictionary<string, TResult>> onEval);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        Lazy<TResult> Load<TResult>(string id);

        /// <summary>
        ///     Loads the specified entity with the specified id and a function to call when it is evaluated
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        Lazy<TResult> Load<TResult>(string id, Action<TResult> onEval);

        /// <summary>
        ///     Loads multiple entities that contain common prefix.
        /// </summary>
        /// <param name="idPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document IDs (after 'idPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document IDs (after 'idPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="startAfter">
        ///     skip document fetching until given ID is found and return documents after that ID (default:
        ///     null)
        /// </param>
        Lazy<Dictionary<string, TResult>> LoadStartingWith<TResult>(string idPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, string startAfter = null);

        /// <summary>
        ///     Loads the specified entity with the specified id and changeVector.
        ///     If the entity is loaded into the session, the tracked entity will be returned otherwise the entity will be loaded only if it is fresher then the provided changeVector.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be conditional loaded.</param>
        /// <param name="changeVector">Change vector of a entity that will be conditional loaded.</param>
        Lazy<(T Entity, string ChangeVector)> ConditionalLoad<T>(string id, string changeVector);
    }

    /// <summary>
    ///     Specify interface for lazy async operation for the session
    /// </summary>
    public interface IAsyncLazySessionOperations
    {
        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLazyLoaderWithInclude<object> Include(string path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLazyLoaderWithInclude<TResult> Include<TResult>(Expression<Func<TResult, string>> path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLazyLoaderWithInclude<TResult> Include<TResult>(Expression<Func<TResult, IEnumerable<string>>> path);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        /// <param name="token">Cancellation token</param>
        Lazy<Task<Dictionary<string, TResult>>> LoadAsync<TResult>(IEnumerable<string> ids, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads the specified entities with the specified ids and a function to call when it is evaluated
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        /// <param name="token">Cancellation token</param>
        Lazy<Task<Dictionary<string, TResult>>> LoadAsync<TResult>(IEnumerable<string> ids, Action<Dictionary<string, TResult>> onEval, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        /// <param name="token">Cancellation token</param>
        Lazy<Task<TResult>> LoadAsync<TResult>(string id, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads the specified entity with the specified id and a function to call when it is evaluated
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        /// <param name="token">Cancellation token</param>
        Lazy<Task<TResult>> LoadAsync<TResult>(string id, Action<TResult> onEval, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads multiple entities that contain common prefix.
        /// </summary>
        /// <param name="idPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document IDs (after 'idPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document IDs (after 'idPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="startAfter">
        ///     skip document fetching until given ID is found and return documents after that ID (default:
        ///     null)
        /// </param>
        /// <param name="token">Cancellation token</param>
        Lazy<Task<Dictionary<string, TResult>>> LoadStartingWithAsync<TResult>(string idPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, string startAfter = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads the specified entity with the specified id and changeVector.
        ///     If the entity is loaded into the session, the tracked entity will be returned otherwise the entity will be loaded only if it is fresher then the provided changeVector.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be conditional loaded.</param>
        /// <param name="changeVector">Change vector of a entity that will be conditional loaded.</param>
        /// <param name="token">The cancellation token.</param>
        Lazy<Task<(T Entity, string ChangeVector)>> ConditionalLoadAsync<T>(string id, string changeVector, CancellationToken token = default(CancellationToken));
    }

    /// <summary>
    ///     Allow to perform eager operations on the session
    /// </summary>
    public interface IEagerSessionOperations
    {
        /// <summary>
        ///     Execute all the lazy requests pending within this session
        /// </summary>
        ResponseTimeInformation ExecuteAllPendingLazyOperations();
    }

    public interface IAsyncEagerSessionOperations
    {
        /// <summary>
        ///     Execute all the lazy requests pending within this session
        /// </summary>
        Task<ResponseTimeInformation> ExecuteAllPendingLazyOperationsAsync(CancellationToken token = default(CancellationToken));
    }
}
