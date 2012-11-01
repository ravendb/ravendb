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
	/// Fluent implementation for specifying include paths
	/// for loading documents
	/// </summary>
	public class AsyncMultiLoaderWithInclude<T> : IAsyncLoaderWithInclude<T>
	{
		private readonly IAsyncDocumentSessionImpl session;
		private readonly List<string> includes = new List<string>();

		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		public AsyncMultiLoaderWithInclude<T> Include(string path)
		{
			includes.Add(path);
			return this;
		}

		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		public AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, object>> path)
		{
			return Include(path.ToPropertyPath());
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		public Task<T[]> LoadAsync(params string[] ids)
		{
			return session.LoadAsyncInternal<T>(ids, includes.ToArray());
		}

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <param name="id">The id.</param>
		public Task<T> Load(string id)
		{
			return session.LoadAsyncInternal<T>(new[] {id}, includes.ToArray()).ContinueWith(x => x.Result.FirstOrDefault());
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
		public Task<T> Load(ValueType id)
		{
			var idAsStr = session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof (T), false);
			return Load(idAsStr);
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
		/// Loads the specified ids.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="ids">The ids.</param>
		public Task<TResult[]> Load<TResult>(params string[] ids)
		{
			return session.LoadAsyncInternal<TResult>(ids, includes.ToArray());
		}

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="id">The id.</param>
		public Task<TResult> Load<TResult>(string id)
		{
			return Load<TResult>(new[] { id }).ContinueWith(x => x.Result.FirstOrDefault());
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
		public Task<TResult> Load<TResult>(ValueType id)
		{
			var idAsStr = session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return Load<TResult>(new[] { idAsStr }).ContinueWith(x => x.Result.FirstOrDefault());
		}
	}
}