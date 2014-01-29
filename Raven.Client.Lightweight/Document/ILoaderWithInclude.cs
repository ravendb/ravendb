#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="ILoaderWithInclude.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Raven.Client.Document
{
	/// <summary>
	/// Fluent interface for specifying include paths
	/// for loading documents
	/// </summary>
	public interface ILoaderWithInclude<T>
	{
		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		ILoaderWithInclude<T> Include(string path);

		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		ILoaderWithInclude<T> Include(Expression<Func<T, object>> path);

		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		ILoaderWithInclude<T> Include<TInclude>(Expression<Func<T, object>> path);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		T[] Load(params string[] ids);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		T[] Load(IEnumerable<string> ids);

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		T Load(string id);

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
		T Load(ValueType id);

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
		T[] Load(params ValueType[] ids);

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
		T[] Load(IEnumerable<ValueType> ids);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		TResult[] Load<TResult>(params string[] ids);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		TResult[] Load<TResult>(IEnumerable<string> ids);

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		TResult Load<TResult>(string id);

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
		TResult Load<TResult>(ValueType id);

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
		TResult[] Load<TResult>(params ValueType[] ids);

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
		TResult[] Load<TResult>(IEnumerable<ValueType> ids);
	}
}

#endif
