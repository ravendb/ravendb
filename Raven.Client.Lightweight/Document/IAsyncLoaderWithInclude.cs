using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Raven.Client.Document
{
	/// <summary>
	/// Fluent interface for specifying include paths
	/// for loading documents
	/// </summary>
	public interface IAsyncLoaderWithInclude<T>
	{
		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		AsyncMultiLoaderWithInclude<T> Include(string path);

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, object>> path);

		/// <summary>
		/// Begins the async multi-load operation
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Task<T[]> LoadAsync(params string[] ids);

		/// <summary>
		/// Begins the async multi-load operation
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Task<T[]> LoadAsync(IEnumerable<string> ids);

		/// <summary>
		/// Begins the async load operation
		/// </summary>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		Task<T> LoadAsync(string id);

		/// <summary>
		/// Begins the async load operation, with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(1)
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<T> LoadAsync(ValueType id);

		/// <summary>
		/// Begins the async multi-load operation, with the specified ids after applying
		/// conventions on the provided ids to get the real document ids.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(1,2,3)
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<T[]> LoadAsync(params ValueType[] ids);

		/// <summary>
		/// Begins the async multi-load operation, with the specified ids after applying
		/// conventions on the provided ids to get the real document ids.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<T[]> LoadAsync(IEnumerable<ValueType> ids);

		/// <summary>
		/// Begins the async multi-load operation
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Task<TResult[]> LoadAsync<TResult>(params string[] ids);

		/// <summary>
		/// Begins the async multi-load operation
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids);

		/// <summary>
		/// Begins the async load operation
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		Task<TResult> LoadAsync<TResult>(string id);

		/// <summary>
		/// Begins the async load operation, with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(1)
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<TResult> LoadAsync<TResult>(ValueType id);

		/// <summary>
		/// Begins the async multi-load operation, with the specified ids after applying
		/// conventions on the provided ids to get the real document ids.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(1,2,3)
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<TResult[]> LoadAsync<TResult>(params ValueType[] ids);

		/// <summary>
		/// Begins the async multi-load operation, with the specified ids after applying
		/// conventions on the provided ids to get the real document ids.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
		/// And that call will internally be translated to 
		/// LoadAsync{Post}("posts/1","posts/2","posts/3");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		Task<TResult[]> LoadAsync<TResult>(IEnumerable<ValueType> ids);
	}
}
