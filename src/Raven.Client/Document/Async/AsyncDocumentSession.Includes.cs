//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Connection;

using Raven.NewClient.Client.Linq;
using Raven.NewClient.Client.Indexes;

using Raven.NewClient.Client.Document.Batches;
using System.Diagnostics;
using System.Dynamic;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Document.Async
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations, IDocumentQueryGenerator
    {
        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        public IAsyncLoaderWithInclude<object> Include(string path)
        {
            return new AsyncMultiLoaderWithInclude<object>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        public IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, string>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        public IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, string>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include<TInclude>(path);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        public IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        public IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include<TInclude>(path);
        }
    }
}
