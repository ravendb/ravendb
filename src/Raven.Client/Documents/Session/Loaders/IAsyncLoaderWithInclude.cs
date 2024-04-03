using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session.Loaders
{
    /// <summary>
    /// Fluent interface for specifying include paths
    /// for loading documents
    /// </summary>
    public interface IAsyncLoaderWithInclude<T>
    {
        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(string)"/>
        AsyncMultiLoaderWithInclude<T> Include(string path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(Expression{Func{T, string}})"/>
        AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, string>> path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments{TResult}(Expression{Func{T, string}})"/>
        /// <typeparam name="TResult">Type of included document.</typeparam>
        AsyncMultiLoaderWithInclude<T> Include<TResult>(Expression<Func<T, string>> path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(Expression{Func{T, IEnumerable{string}}})"/>
        AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, IEnumerable<string>>> path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(Expression{Func{T, IEnumerable{string}}})"/>
        /// <typeparam name="TResult">Type of included document.</typeparam>
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
        Task<Dictionary<string, T>> LoadAsync(IEnumerable<string> ids, CancellationToken token = default);

        /// <summary>
        /// Begins the async load operation
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        Task<T> LoadAsync(string id, CancellationToken token = default);

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
        Task<Dictionary<string, TResult>> LoadAsync<TResult>(IEnumerable<string> ids, CancellationToken token = default);

        /// <summary>
        /// Begins the async load operation
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        Task<TResult> LoadAsync<TResult>(string id, CancellationToken token = default);
    }
}
