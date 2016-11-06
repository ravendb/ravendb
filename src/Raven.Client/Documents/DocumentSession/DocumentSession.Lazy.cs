//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Data.Queries;
using Raven.Client.Documents.SessionOperations;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession : InMemoryDocumentSessionOperations, IDocumentQueryGenerator, ISyncAdvancedSessionOperation, IDocumentSessionImpl
    {
        ILazyLoaderWithInclude<TResult> ILazySessionOperations.Include<TResult>(Expression<Func<TResult, object>> path)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TResult>(IEnumerable<string> ids)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids, Action<TResult[]> onEval)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult> ILazySessionOperations.Load<TResult>(string id)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult> Load<TResult>(string id, Action<TResult> onEval)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult> ILazySessionOperations.Load<TResult>(ValueType id)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult> Load<TResult>(ValueType id, Action<TResult> onEval)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TResult>(params ValueType[] ids)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TResult>(IEnumerable<ValueType> ids)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult[]> Load<TResult>(IEnumerable<ValueType> ids, Action<TResult[]> onEval)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult> ILazySessionOperations.Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure, Action<TResult> onEval)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult> Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult[]> Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult[]> ILazySessionOperations.LoadStartingWith<TResult>(string keyPrefix, string matches, int start, int pageSize,
            string exclude, RavenPagingInformation pagingInformation, string skipAfter)
        {
            throw new NotImplementedException();
        }


        public Lazy<TResult[]> MoreLikeThis<TResult>(MoreLikeThisQuery query)
        {
            throw new NotImplementedException();
        }

        ILazyLoaderWithInclude<object> ILazySessionOperations.Include(string path)
        {
            throw new NotImplementedException();
        }

        public Lazy<T[]> LazyLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval)
        {
            throw new NotImplementedException();
        }
    }
}