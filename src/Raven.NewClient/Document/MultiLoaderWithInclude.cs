//-----------------------------------------------------------------------
// <copyright file="MultiLoaderWithInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexes;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Fluent implementation for specifying include paths
    /// for loading documents
    /// </summary>
    public class MultiLoaderWithInclude<T> : ILoaderWithInclude<T>
    {
        private readonly IDocumentSessionImpl session;
        private readonly List<string> includes = new List<string>();

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public ILoaderWithInclude<T> Include(string path)
        {
            return Include(path, typeof(object));
        }

        ILoaderWithInclude<T> Include(string path, Type type)
        {
            includes.Add(path);
            return this;
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public ILoaderWithInclude<T> Include(Expression<Func<T, string>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public ILoaderWithInclude<T> Include<TInclude>(Expression<Func<T, string>> path)
        {
            return Include(path.ToPropertyPath(), typeof(TInclude));
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public ILoaderWithInclude<T> Include(Expression<Func<T, IEnumerable<string>>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        public ILoaderWithInclude<T> Include<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return Include(path.ToPropertyPath(), typeof(TInclude));
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Dictionary<string, T> Load(params string[] ids)
        {
            return session.LoadInternal<T>(ids, includes.ToArray());
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public Dictionary<string, T> Load(IEnumerable<string> ids)
        {
            return session.LoadInternal<T>(ids.ToArray(), includes.ToArray());
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <param name="id">The id.</param>
        public T Load(string id)
        {
            return session.LoadInternal<T>(new[] { id }, includes.ToArray()).Values.FirstOrDefault();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiLoaderWithInclude{T}"/> class.
        /// </summary>
        /// <param name="session">The session.</param>
        internal MultiLoaderWithInclude(IDocumentSessionImpl session)
        {
            this.session = session;
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="ids">The ids.</param>
        public Dictionary<string, TResult> Load<TResult>(params string[] ids)
        {
            return session.LoadInternal<TResult>(ids, includes.ToArray());
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="ids">The ids.</param>
        public Dictionary<string, TResult> Load<TResult>(IEnumerable<string> ids)
        {
            return session.LoadInternal<TResult>(ids.ToArray(), includes.ToArray());
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        public TResult Load<TResult>(string id)
        {
            return Load<TResult>(new[] { id }).Values.FirstOrDefault();
        }

        public TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null)
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new RavenLoadConfiguration();
            configure?.Invoke(configuration);

            return session.LoadInternal<TResult>(new[] { id }, includes.ToArray(), transformer, configuration.TransformerParameters).FirstOrDefault();
        }

        public TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null)
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new RavenLoadConfiguration();
            configure?.Invoke(configuration);

            return session.LoadInternal<TResult>(ids.ToArray(), includes.ToArray(), transformer, configuration.TransformerParameters);
        }
    }
}
