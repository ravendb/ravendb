//-----------------------------------------------------------------------
// <copyright file="MultiLoaderWithInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session.Loaders
{
    /// <summary>
    /// Fluent implementation for specifying include paths for loading documents
    /// </summary>
    public class AsyncMultiLoaderWithInclude<T> : IAsyncLoaderWithInclude<T>
    {
        private readonly IAsyncDocumentSessionImpl _session;
        private readonly List<string> _includes = new List<string>();

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public AsyncMultiLoaderWithInclude<T> Include(string path)
        {
            _includes.Add(path);
            return this;
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, string>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public AsyncMultiLoaderWithInclude<T> Include<TInclude>(Expression<Func<T, string>> path)
        {
            return Include(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _session.Conventions));
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, IEnumerable<string>>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public AsyncMultiLoaderWithInclude<T> Include<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return Include(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _session.Conventions));
        }

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Task<Dictionary<string, T>> LoadAsync(params string[] ids)
        {
            return _session.LoadAsyncInternal<T>(ids, _includes.ToArray());
        }

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Task<Dictionary<string, T>> LoadAsync(IEnumerable<string> ids)
        {
            return _session.LoadAsyncInternal<T>(ids.ToArray(), _includes.ToArray());
        }

        /// <summary>
        /// Begins the async load operation
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        public Task<T> LoadAsync(string id)
        {
            return _session.LoadAsyncInternal<T>(new[] { id }, _includes.ToArray()).ContinueWith(x => x.Result.Values.FirstOrDefault());
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncMultiLoaderWithInclude{T}"/> class.
        /// </summary>
        /// <param name="session">The session.</param>
        internal AsyncMultiLoaderWithInclude(IAsyncDocumentSessionImpl session)
        {
            _session = session;
        }

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Task<Dictionary<string, TResult>> LoadAsync<TResult>(params string[] ids)
        {
            return _session.LoadAsyncInternal<TResult>(ids, _includes.ToArray());
        }

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Task<Dictionary<string, TResult>> LoadAsync<TResult>(IEnumerable<string> ids)
        {
            return _session.LoadAsyncInternal<TResult>(ids.ToArray(), _includes.ToArray());
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        public Task<TResult> LoadAsync<TResult>(string id)
        {
            return LoadAsync<TResult>(new[] { id }).ContinueWith(x => x.Result.Values.FirstOrDefault());
        }
    }
}

