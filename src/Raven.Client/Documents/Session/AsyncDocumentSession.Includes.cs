//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Session.Loaders;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession
    {
        /// <inheritdoc/>
        public IAsyncLoaderWithInclude<object> Include(string path)
        {
            return new AsyncMultiLoaderWithInclude<object>(this).Include(path);
        }

        /// <inheritdoc/>
        public IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, string>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <inheritdoc/>
        public IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, string>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include<TInclude>(path);
        }

        /// <inheritdoc/>
        public IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <inheritdoc/>
        public IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return new AsyncMultiLoaderWithInclude<T>(this).Include<TInclude>(path);
        }
    }
}
