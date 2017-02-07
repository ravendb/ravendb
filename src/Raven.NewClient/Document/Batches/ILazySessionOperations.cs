using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Indexes;

namespace Raven.NewClient.Client.Document.Batches
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
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a document to load</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        Lazy<TResult> Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a entity to load</param>
        /// <param name="transformerType">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        Lazy<TResult> Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null);

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        Lazy<TResult[]> Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="transformerType">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null);

        /// <summary>
        ///     Loads multiple entities that contain common prefix.
        /// </summary>
        /// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="skipAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        Lazy<TResult[]> LoadStartingWith<TResult>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null);

        Lazy<List<TResult>> MoreLikeThis<TResult>(MoreLikeThisQuery query);
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

        // <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLazyLoaderWithInclude<TResult> Include<TResult>(Expression<Func<TResult, IEnumerable<string>>> path);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        /// <param name="token">The cancellation token.</param>
        Lazy<Task<Dictionary<string, TResult>>> LoadAsync<TResult>(IEnumerable<string> ids, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Loads the specified entities with the specified ids and a function to call when it is evaluated.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        /// <param name="token">The cancellation token.</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        Lazy<Task<Dictionary<string, TResult>>> LoadAsync<TResult>(IEnumerable<string> ids, Action<Dictionary<string, TResult>> onEval, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        /// <param name="token">The cancellation token.</param>
        Lazy<Task<TResult>> LoadAsync<TResult>(string id, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Loads the specified entity with the specified id and a function to call when it is evaluated.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        /// <param name="onEval">Action to be called on evalution.</param>
        /// <param name="token">The cancellation token.</param>
        Lazy<Task<TResult>> LoadAsync<TResult>(string id, Action<TResult> onEval, CancellationToken token = default (CancellationToken));
        
        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a document to load</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="onEval">Action to be called on evalution.</param>
        /// <param name="token">The cancellation token.</param>
        Lazy<Task<TResult>> LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null, CancellationToken token = default (CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a entity to load</param>
        /// <param name="transformerType">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        /// <param name="onEval">Action to be executed on evaluation.</param>
        Lazy<Task<TResult>> LoadAsync<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Loads multiple entities that contain common prefix.
        /// </summary>
        /// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="skipAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Lazy<Task<TResult[]>> LoadStartingWithAsync<TResult>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null, CancellationToken token = default (CancellationToken));

        Lazy<Task<List<TResult>>> MoreLikeThisAsync<TResult>(MoreLikeThisQuery query, CancellationToken token = default (CancellationToken));
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
        Task<ResponseTimeInformation> ExecuteAllPendingLazyOperationsAsync(CancellationToken token = default (CancellationToken));
    }
}
