//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        ILazyLoaderWithInclude<T> ILazySessionOperations.Include<T>(Expression<Func<T, string>> path)
        {
            return new LazyMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        ILazyLoaderWithInclude<T> ILazySessionOperations.Include<T>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return new LazyMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        Lazy<Dictionary<string, T>> ILazySessionOperations.Load<T>(IEnumerable<string> ids)
        {
            return Lazily.Load<T>(ids, null);
        }

        /// <summary>
        /// Loads the specified ids and a function to call when it is evaluated
        /// </summary>
        Lazy<Dictionary<string, T>> ILazySessionOperations.Load<T>(IEnumerable<string> ids, Action<Dictionary<string, T>> onEval)
        {
            return LazyLoadInternal(ids.ToArray(), new string[0], onEval);
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        Lazy<T> ILazySessionOperations.Load<T>(string id)
        {
            return Lazily.Load(id, (Action<T>)null);
        }

        /// <summary>
        /// Loads the specified id and a function to call when it is evaluated
        /// </summary>
        Lazy<T> ILazySessionOperations.Load<T>(string id, Action<T> onEval)
        {
            if (IsLoaded(id))
                return new Lazy<T>(() => Load<T>(id));
            var lazyLoadOperation = new LazyLoadOperation<T>(this, new LoadOperation(this).ById(id)).ById(id);
            return AddLazyOperation(lazyLoadOperation, onEval);
        }

        internal Lazy<T> AddLazyOperation<T>(ILazyOperation operation, Action<T> onEval)
        {
            PendingLazyOperations.Add(operation);
            var lazyValue = new Lazy<T>(() =>
            {
                ExecuteAllPendingLazyOperations();
                return GetOperationResult<T>(operation.Result);
            });

            if (onEval != null)
                OnEvaluateLazy[operation] = theResult => onEval(GetOperationResult<T>(theResult));

            return lazyValue;
        }

        Lazy<Dictionary<string, TResult>> ILazySessionOperations.LoadStartingWith<TResult>(string idPrefix, string matches, int start, int pageSize, string exclude, string startAfter)
        {
            var operation = new LazyStartsWithOperation<TResult>(idPrefix, matches, exclude, start, pageSize, this, startAfter);

            return AddLazyOperation<Dictionary<string, TResult>>(operation, null);
        }

        Lazy<(T Entity, string ChangeVector)> ILazySessionOperations.ConditionalLoad<T>(string id, string changeVector)
        {
            if (string.IsNullOrEmpty(id))
                throw new ArgumentNullException(nameof(id));

            if (IsLoaded(id))
            {
                return new Lazy<(T Entity, string ChangeVector)>(() =>
                {
                    var entity = Load<T>(id);
                    if (entity == null)
                        return default;

                    var cv = Advanced.GetChangeVectorFor(entity);
                    return (entity, cv);
                });
            }

            if (string.IsNullOrEmpty(changeVector))
                throw new InvalidOperationException($"The requested document with id '{id} is not loaded into the session and could not conditional load when {nameof(changeVector)} is null or empty.");

            var lazyLoadOperation = new LazyConditionalLoadOperation<T>(id, changeVector, this);
            return AddLazyOperation<(T Entity, string ChangeVector)>(lazyLoadOperation, null);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        ILazyLoaderWithInclude<object> ILazySessionOperations.Include(string path)
        {
            return new LazyMultiLoaderWithInclude<object>(this).Include(path);
        }

        /// <summary>
        /// Register to lazily load documents and include
        /// </summary>
        public Lazy<Dictionary<string, T>> LazyLoadInternal<T>(string[] ids, string[] includes, Action<Dictionary<string, T>> onEval)
        {
            if (CheckIfIdAlreadyIncluded(ids, includes))
            {
                return new Lazy<Dictionary<string, T>>(() => Load<T>(ids));
            }

            var loadOperation = new LoadOperation(this)
                .ByIds(ids)
                .WithIncludes(includes);

            var lazyOp = new LazyLoadOperation<T>(this, loadOperation).ByIds(ids).WithIncludes(includes);
            return AddLazyOperation(lazyOp, onEval);
        }

        internal Lazy<int> AddLazyCountOperation(ILazyOperation operation)
        {
            PendingLazyOperations.Add(operation);
            var lazyValue = new Lazy<int>(() =>
            {
                ExecuteAllPendingLazyOperations();
                return operation.QueryResult.TotalResults;
            });

            return lazyValue;
        }
    }
}
