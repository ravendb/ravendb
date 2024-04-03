using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session.Loaders
{
    /// <summary>
    ///     Fluent interface for specifying include paths
    ///     for loading documents lazily
    /// </summary>
    public interface ILazyLoaderWithInclude<T>
    {
        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(string)"/>
        ILazyLoaderWithInclude<T> Include(string path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(Expression{Func{T, string}})"/>
        ILazyLoaderWithInclude<T> Include(Expression<Func<T, string>> path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(Expression{Func{T, IEnumerable{string}}})"/>
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
        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(string)"/>
        IAsyncLazyLoaderWithInclude<T> Include(string path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(Expression{Func{T, string}})"/>
        IAsyncLazyLoaderWithInclude<T> Include(Expression<Func<T, string>> path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(Expression{Func{T, IEnumerable{string}}})"/>
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
