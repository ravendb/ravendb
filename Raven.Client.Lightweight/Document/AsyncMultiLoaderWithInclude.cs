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
using Raven.Abstractions.Extensions;

namespace Raven.Client.Document
{
    /// <summary>
    /// Fluent implementation for specifying include paths for loading documents
    /// </summary>
    public class AsyncMultiLoaderWithInclude<T> : IAsyncLoaderWithInclude<T>
    {
        private readonly IAsyncDocumentSessionImpl session;
        private readonly List<KeyValuePair<string, Type>> includes = new List<KeyValuePair<string, Type>>();

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public AsyncMultiLoaderWithInclude<T> Include(string path)
        {
            return Include(path, typeof(object));
        }

        AsyncMultiLoaderWithInclude<T> Include(string path, Type type)
        {
            includes.Add(new KeyValuePair<string, Type>(path, type));
            return this;
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, object>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public AsyncMultiLoaderWithInclude<T> Include<TInclude>(Expression<Func<T, object>> path)
        {
            var fullId = session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(-1, typeof(TInclude), false);
            var id = path.ToPropertyPath();
            var idPrefix = fullId.Replace("-1", string.Empty);

            id += "(" + idPrefix + ")";

            return Include(id, typeof(TInclude));
        }

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Task<T[]> LoadAsync(params string[] ids)
        {
            return session.LoadAsyncInternal<T>(ids, includes.ToArray());
        }

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Task<T[]> LoadAsync(IEnumerable<string> ids)
        {
            return session.LoadAsyncInternal<T>(ids.ToArray(), includes.ToArray());
        }

        /// <summary>
        /// Begins the async load operation
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        public Task<T> LoadAsync(string id)
        {
            return session.LoadAsyncInternal<T>(new[] { id }, includes.ToArray()).ContinueWith(x => x.Result.FirstOrDefault());
        }

        /// <summary>
        /// Begins the async load operation, with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(1)
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T> LoadAsync(ValueType id)
        {
            var documentKey = session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return LoadAsync(documentKey);
        }

        /// <summary>
        /// Begins the async multi-load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(1,2,3)
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T[]> LoadAsync(params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsync(documentKeys);
        }

        /// <summary>
        /// Begins the async multi-load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T[]> LoadAsync(IEnumerable<ValueType> ids)
        {
            var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsync(documentKeys);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncMultiLoaderWithInclude{T}"/> class.
        /// </summary>
        /// <param name="session">The session.</param>
        public AsyncMultiLoaderWithInclude(IAsyncDocumentSessionImpl session)
        {
            this.session = session;
        }

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Task<TResult[]> LoadAsync<TResult>(params string[] ids)
        {
            return session.LoadAsyncInternal<TResult>(ids, includes.ToArray());
        }

        /// <summary>
        /// Begins the async multi-load operation
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids)
        {
            return session.LoadAsyncInternal<TResult>(ids.ToArray(), includes.ToArray());
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        public Task<TResult> LoadAsync<TResult>(string id)
        {
            return LoadAsync<TResult>(new[] { id }).ContinueWith(x => x.Result.FirstOrDefault());
        }

        /// <summary>
        /// Begins the async load operation, with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(1)
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<TResult> LoadAsync<TResult>(ValueType id)
        {
            var documentKey = session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(TResult), false);
            return LoadAsync<TResult>(documentKey);
        }

        /// <summary>
        /// Begins the async multi-load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(1,2,3)
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<TResult[]> LoadAsync<TResult>(params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsync<TResult>(documentKeys);
        }

        /// <summary>
        /// Begins the async multi-load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<TResult[]> LoadAsync<TResult>(IEnumerable<ValueType> ids)
        {
            var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsync<TResult>(documentKeys);
        }
    }
}
