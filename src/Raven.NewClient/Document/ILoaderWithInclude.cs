//-----------------------------------------------------------------------
// <copyright file="ILoaderWithInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.NewClient.Client.Indexes;

namespace Raven.NewClient.Client.Document
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

        /// <summary>
        /// Loads the specified id with a specific transformer.
        /// </summary>
        /// <typeparam name="TTransformer"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <returns></returns>
        TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        /// Loads the specified ids with a specific transformer.
        /// </summary>
        /// <typeparam name="TTransformer"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="ids">The id.</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <returns></returns>
        TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new();
    }
}
