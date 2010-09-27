using System;
using System.Linq.Expressions;

namespace Raven.Client.Document
{
    /// <summary>
    /// Fluent interface for specifying include paths
    /// for loading documents
    /// </summary>
    public interface ILoaderWithInclude<T>
    {
        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        MultiLoaderWithInclude<T> Include(string path);

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        MultiLoaderWithInclude<T> Include(Expression<Func<T, object>> path);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        T[] Load(params string[] ids);

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        T Load(string id);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        TResult[] Load<TResult>(params string[] ids);

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        TResult Load<TResult>(string id);
    }
}