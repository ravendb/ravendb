using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Database.Extensions;

namespace Raven.Client.Document
{
    /// <summary>
    /// Fluent implementation for specifying include paths
    /// for loading documents
    /// </summary>
    public class MultiLoaderWithInclude<T> : ILoaderWithInclude<T>
    {
        private readonly DocumentSession session;
        private readonly List<string> includes = new List<string>();

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public MultiLoaderWithInclude<T> Include(string path)
        {
            includes.Add(path);
            return this;
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public MultiLoaderWithInclude<T> Include(Expression<Func<T, object>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public T[] Load(params string[] ids)
        {
            return session.LoadInternal<T>(ids, includes.ToArray());
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <param name="id">The id.</param>
        public T Load(string id)
        {
            return session.LoadInternal<T>(new[] { id }, includes.ToArray()).FirstOrDefault();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiLoaderWithInclude{T}"/> class.
        /// </summary>
        /// <param name="session">The session.</param>
        public MultiLoaderWithInclude(DocumentSession session)
        {
            this.session = session;
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="ids">The ids.</param>
        public TResult[] Load<TResult>(params string[] ids)
        {
            return session.LoadInternal<TResult>(ids, includes.ToArray());
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        public TResult Load<TResult>(string id)
        {
            return Load<TResult>(new[] {id}).FirstOrDefault();
        }
    }
}
