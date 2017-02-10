using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Document.Batches
{
    /// <summary>
    ///     Fluent interface for specifying include paths
    ///     for loading documents lazily
    /// </summary>
    public interface ILazyLoaderWithInclude<T>
    {
        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        ILazyLoaderWithInclude<T> Include(string path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        ILazyLoaderWithInclude<T> Include(Expression<Func<T, string>> path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        ILazyLoaderWithInclude<T> Include(Expression<Func<T, IEnumerable<string>>> path);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Dictionary<string, T>> Load(params string[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Dictionary<string, T>> Load(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        Lazy<T> Load(string id);
        
        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Dictionary<string, TResult>> Load<TResult>(params string[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Dictionary<string, TResult>> Load<TResult>(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        Lazy<TResult> Load<TResult>(string id);
    }

    public interface IAsyncLazyLoaderWithInclude<T>
    {
        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLazyLoaderWithInclude<T> Include(string path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLazyLoaderWithInclude<T> Include(Expression<Func<T, string>> path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLazyLoaderWithInclude<T> Include(Expression<Func<T, IEnumerable<string>>> path);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Task<Dictionary<string, T>>> LoadAsync(params string[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Task<Dictionary<string, T>>> LoadAsync(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        Lazy<Task<T>> LoadAsync(string id);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Task<Dictionary<string, TResult>>> LoadAsync<TResult>(params string[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Task<Dictionary<string, TResult>>> LoadAsync<TResult>(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        Lazy<Task<TResult>> LoadAsync<TResult>(string id);
    }
}
