#if !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Document.Batches
{
	public class LazyMultiLoaderWithInclude<T> : ILazyLoaderWithInclude<T>
	{
		private readonly IDocumentSessionImpl session;
		private readonly List<KeyValuePair<string, Type>> includes = new List<KeyValuePair<string, Type>>();

		/// <summary>
		/// Initializes a new instance of the <see cref="LazyMultiLoaderWithInclude{T}"/> class.
		/// </summary>
		/// <param name="session">The session.</param>
		internal LazyMultiLoaderWithInclude(IDocumentSessionImpl session)
		{
			this.session = session;
		}

		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		public ILazyLoaderWithInclude<T> Include(string path)
		{
			includes.Add(new KeyValuePair<string, Type>(path, typeof(object)));
			return this;
		}

		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		public ILazyLoaderWithInclude<T> Include(Expression<Func<T, object>> path)
		{
			return Include(path.ToPropertyPath());
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		public Lazy<T[]> Load(params string[] ids)
		{
			return session.LazyLoadInternal<T>(ids, includes.ToArray(), null);
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		public Lazy<T[]> Load(IEnumerable<string> ids)
		{
			return session.LazyLoadInternal<T>(ids.ToArray(), includes.ToArray(), null);
		}

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		public Lazy<T> Load(string id)
		{
			var results = session.LazyLoadInternal<T>(new[] { id }, includes.ToArray(), null);
			return new Lazy<T>(() => results.Value.First());
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
		public Lazy<T> Load(ValueType id)
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
		public Lazy<T[]> Load(params ValueType[] ids)
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
		public Lazy<T[]> Load(IEnumerable<ValueType> ids)
		{
			var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Load(documentKeys);
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="ids">The ids.</param>
		public Lazy<TResult[]> Load<TResult>(params string[] ids)
		{
			return session.LazyLoadInternal<TResult>(ids, includes.ToArray(), null);
		}

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		public Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids)
		{
			return session.LazyLoadInternal<TResult>(ids.ToArray(), includes.ToArray(), null);
		}

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="id">The id.</param>
		public Lazy<TResult> Load<TResult>(string id)
		{
			var lazy = Load<TResult>(new[] { id });
			return new Lazy<TResult>(() => lazy.Value.FirstOrDefault());
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
		public Lazy<TResult> Load<TResult>(ValueType id)
		{
			var documentKey = session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
			return Load<TResult>(documentKey);
		}

		public Lazy<TResult[]> Load<TResult>(params ValueType[] ids)
		{
			var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Load<TResult>(documentKeys);
		}

		public Lazy<TResult[]> Load<TResult>(IEnumerable<ValueType> ids)
		{
			var documentKeys = ids.Select(id => session.Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
			return Load<TResult>(documentKeys);
		}
	}
}

#endif
