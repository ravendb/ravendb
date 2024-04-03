//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
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
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments{TInclude}(Expression{System.Func{T,string}})"/>
        /// <typeparam name="T">Type of main loaded document</typeparam>
        public ILoaderWithInclude<T> Include<T>(Expression<Func<T, string>> path)
        {
            return new MultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments{TInclude}(Expression{System.Func{T,string}})"/>
        /// <typeparam name="T">Type of main loaded document</typeparam>
        /// <typeparam name="TInclude">Type of included document</typeparam>
        public ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, string>> path)
        {
            return new MultiLoaderWithInclude<T>(this).Include<TInclude>(path);
        }

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments{TInclude}(Expression{Func{T,IEnumerable{string}}})"/>
        /// <typeparam name="T">Type of main loaded document</typeparam>
        public ILoaderWithInclude<T> Include<T>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return new MultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments{TInclude}(Expression{Func{T,IEnumerable{string}}})"/>
        /// <typeparam name="T">Type of main loaded document</typeparam>
        /// <typeparam name="TInclude">Type of included document</typeparam>
        public ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return new MultiLoaderWithInclude<T>(this).Include<TInclude>(path);
        }

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(string)"/>
        public ILoaderWithInclude<object> Include(string path)
        {
            return new MultiLoaderWithInclude<object>(this).Include(path);
        }
    }
}
