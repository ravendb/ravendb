using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.NewClient.Client.Document.Async;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Fluent interface for specifying include paths
    /// for loading documents
    /// </summary>
    public interface IAsyncLoaderWithInclude<T>
    {
        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        AsyncMultiLoaderWithInclude<T> Include(string path);

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, string>> path);
        /// <summary>
        /// Begin a load while including the specified path
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        AsyncMultiLoaderWithInclude<T> Include<TResult>(Expression<Func<T, string>> path);

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, IEnumerable<string>>> path);
        /// <summary>
        /// Begin a load while including the specified path
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        AsyncMultiLoaderWithInclude<T> Include<TResult>(Expression<Func<T, IEnumerable<string>>> path);

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Task<Dictionary<string, T>> LoadAsync(params string[] ids);

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Task<Dictionary<string, T>> LoadAsync(IEnumerable<string> ids);

        /// <summary>
        /// Begins the async load operation
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        Task<T> LoadAsync(string id);

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Task<Dictionary<string, TResult>> LoadAsync<TResult>(params string[] ids);

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Task<Dictionary<string, TResult>> LoadAsync<TResult>(IEnumerable<string> ids);

        /// <summary>
        /// Begins the async load operation
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        Task<TResult> LoadAsync<TResult>(string id);
    }
}
