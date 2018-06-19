//-----------------------------------------------------------------------
// <copyright file="ILoaderWithInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Raven.Client.Documents.Session.Loaders
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
        ILoaderWithInclude<T> Include(string path);

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        ILoaderWithInclude<T> Include(Expression<Func<T, string>> path);

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        ILoaderWithInclude<T> Include<TInclude>(Expression<Func<T, string>> path);

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        ILoaderWithInclude<T> Include(Expression<Func<T, IEnumerable<string>>> path);

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        ILoaderWithInclude<T> Include<TInclude>(Expression<Func<T, IEnumerable<string>>> path);

        /// <summary>
        ///     Begin a load while including counter of specified name
        /// </summary>
        /// <param name="name">Name of the counter to include.</param>
        ILoaderWithInclude<T> IncludeCounter(string name);

        /// <summary>
        ///     Begin a load while including all the counters of the document
        /// </summary>
        ILoaderWithInclude<T> IncludeCounters();

        /// <summary>
        ///     Begin a load while including counters of specified names
        /// </summary>
        /// <param name="names">Names of the counters to include.</param>
        ILoaderWithInclude<T> IncludeCounters(string[] names);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Dictionary<string, T> Load(params string[] ids);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Dictionary<string, T> Load(IEnumerable<string> ids);

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
        Dictionary<string, TResult> Load<TResult>(params string[] ids);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Dictionary<string, TResult> Load<TResult>(IEnumerable<string> ids);

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        TResult Load<TResult>(string id);
   }
}
