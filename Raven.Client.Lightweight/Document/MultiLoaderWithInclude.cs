#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="MultiLoaderWithInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Document
{
	/// <summary>
	/// Fluent implementation for specifying include paths
	/// for loading documents
	/// </summary>
	public class MultiLoaderWithInclude<T> : ILoaderWithInclude<T>
	{
		private readonly IDocumentSessionImpl session;
		private readonly List<KeyValuePair<string, Type>> includes = new List<KeyValuePair<string, Type>>();

		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		public MultiLoaderWithInclude<T> Include(string path)
		{
			return Include(path, typeof(object));
		}

		MultiLoaderWithInclude<T> Include(string path, Type type)
		{
			includes.Add(new KeyValuePair<string, Type>(path, type));
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
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		public MultiLoaderWithInclude<T> Include<TInclude>(Expression<Func<T, object>> path)
		{
			var fullId = session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(-1, typeof (TInclude), false);
			var id = path.ToPropertyPath();
			var idPrefix = fullId.Replace("-1", string.Empty);

			id += "(" + idPrefix + ")";

			return Include(id, typeof (TInclude));
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		public T[] Load(params string[] ids)
		{
			return session.LoadInternal<T>(ids, includes.ToArray());
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		public T[] Load(IEnumerable<string> ids)
		{
			return session.LoadInternal<T>(ids.ToArray(), includes.ToArray());
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
		/// Loads the specified entity with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		public T Load(ValueType id)
		{
			var documentKey = session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return Load(documentKey);
		}

		/// <summary>
		/// Loads the specified entities with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1,2,3)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		public T[] Load(params ValueType[] ids)
		{
			var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Load(documentKeys);
		}

		/// <summary>
		/// Loads the specified entities with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(new List&lt;int&gt;(){1,2,3})
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		public T[] Load(IEnumerable<ValueType> ids)
		{
			var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Load(documentKeys);
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
		public TResult[] Load<TResult>(params string[] ids)
		{
			return session.LoadInternal<TResult>(ids, includes.ToArray());
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="ids">The ids.</param>
		public TResult[] Load<TResult>(IEnumerable<string> ids)
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
			return Load<TResult>(new[] { id }).FirstOrDefault();
		}

		/// <summary>
		/// Loads the specified entities with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		public TResult Load<TResult>(ValueType id)
		{
			var documentKey = session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(TResult), false);
			return Load<TResult>(documentKey);
		}

		public TResult[] Load<TResult>(params ValueType[] ids)
		{
			var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Load<TResult>(documentKeys);
		}

		public TResult[] Load<TResult>(IEnumerable<ValueType> ids)
		{
			var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Load<TResult>(documentKeys);
		}
	}
}

#endif
