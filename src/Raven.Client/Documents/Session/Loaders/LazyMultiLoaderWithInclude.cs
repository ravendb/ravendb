using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session.Loaders
{
    internal class LazyMultiLoaderWithInclude<T> : ILazyLoaderWithInclude<T>
    {
        private readonly IDocumentSessionImpl _session;
        private readonly List<string> _includes = new List<string>();

        /// <summary>
        /// Initializes a new instance of the <see cref="LazyMultiLoaderWithInclude{T}"/> class.
        /// </summary>
        /// <param name="session">The session.</param>
        internal LazyMultiLoaderWithInclude(IDocumentSessionImpl session)
        {
            _session = session;
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public ILazyLoaderWithInclude<T> Include(string path)
        {
            _includes.Add(path);
            return this;
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public ILazyLoaderWithInclude<T> Include(Expression<Func<T, string>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public ILazyLoaderWithInclude<T> Include(Expression<Func<T, IEnumerable<string>>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public Lazy<Dictionary<string, T>> Load(params string[] ids)
        {
            return _session.LazyLoadInternal<T>(ids, _includes.ToArray(), null);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Lazy<Dictionary<string, T>> Load(IEnumerable<string> ids)
        {
            return _session.LazyLoadInternal<T>(ids.ToArray(), _includes.ToArray(), null);
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        public Lazy<T> Load(string id)
        {
            var results = _session.LazyLoadInternal<T>(new[] { id }, _includes.ToArray(), null);
            return new Lazy<T>(() => results.Value.Values.First());
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="ids">The ids.</param>
        public Lazy<Dictionary<string, TResult>> Load<TResult>(params string[] ids)
        {
            return _session.LazyLoadInternal<TResult>(ids, _includes.ToArray(), null);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Lazy<Dictionary<string, TResult>> Load<TResult>(IEnumerable<string> ids)
        {
            return _session.LazyLoadInternal<TResult>(ids.ToArray(), _includes.ToArray(), null);
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        public Lazy<TResult> Load<TResult>(string id)
        {
            var lazy = Load<TResult>(new[] { id });
            return new Lazy<TResult>(() => lazy.Value.Values.FirstOrDefault());
        }
    }

    internal class AsyncLazyMultiLoaderWithInclude<T> : IAsyncLazyLoaderWithInclude<T>
    {
        private readonly IAsyncDocumentSessionImpl _session;
        private readonly List<string> _includes = new List<string>();

        /// <summary>
        /// Initializes a new instance of the <see cref="LazyMultiLoaderWithInclude{T}"/> class.
        /// </summary>
        /// <param name="session">The session.</param>
        internal AsyncLazyMultiLoaderWithInclude(IAsyncDocumentSessionImpl session)
        {
            _session = session;
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public IAsyncLazyLoaderWithInclude<T> Include(string path)
        {
            _includes.Add(path);
            return this;
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public IAsyncLazyLoaderWithInclude<T> Include(Expression<Func<T, string>> path)
        {
            return Include(path.ToPropertyPath());
        }


        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public IAsyncLazyLoaderWithInclude<T> Include(Expression<Func<T, IEnumerable<string>>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public Lazy<Task<Dictionary<string, T>>> LoadAsync(params string[] ids)
        {
            return _session.LazyAsyncLoadInternal<T>(ids, _includes.ToArray(), null);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Lazy<Task<Dictionary<string, T>>> LoadAsync(IEnumerable<string> ids)
        {
            return _session.LazyAsyncLoadInternal<T>(ids.ToArray(), _includes.ToArray(), null);
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        public Lazy<Task<T>> LoadAsync(string id)
        {
            var results = _session.LazyAsyncLoadInternal<T>(new[] { id }, _includes.ToArray(), null);

            return new Lazy<Task<T>>(() => results.Value.ContinueWith(x => x.Result.Values.First()));
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="ids">The ids.</param>
        public Lazy<Task<Dictionary<string, TResult>>> LoadAsync<TResult>(params string[] ids)
        {
            return _session.LazyAsyncLoadInternal<TResult>(ids, _includes.ToArray(), null);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Lazy<Task<Dictionary<string, TResult>>> LoadAsync<TResult>(IEnumerable<string> ids)
        {
            return _session.LazyAsyncLoadInternal<TResult>(ids.ToArray(), _includes.ToArray(), null);
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        public Lazy<Task<TResult>> LoadAsync<TResult>(string id)
        {
            var lazy = LoadAsync<TResult>(new[] { id });
            return new Lazy<Task<TResult>>(() => lazy.Value.ContinueWith(x => x.Result.Values.First()));
        }
    }
}

