using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Raven.Client.Document.Batches
{
	/// <summary>
	/// Specify interface for lazy operation for the session
	/// </summary>
	public interface ILazySessionOperations
	{
		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILazyLoaderWithInclude<object> Include(string path);

		/// <summary>
		/// Begin a load while including the specified path 
		/// </summary>
		/// <param name="path">The path.</param>
		ILazyLoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		Lazy<TResult[]> Load<TResult>(params string[] ids);

		/// <summary>
		/// Loads the specified ids and a function to call when it is evaluated
		/// </summary>
		Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids, Action<TResult[]> onEval);

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		Lazy<TResult> Load<TResult>(string id);

		/// <summary>
		/// Loads the specified id and a function to call when it is evaluated
		/// </summary>
		Lazy<TResult> Load<TResult>(string id, Action<TResult> onEval);

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
		Lazy<TResult> Load<TResult>(ValueType id);

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
		Lazy<TResult> Load<TResult>(ValueType id, Action<TResult> onEval);	
	}

	/// <summary>
	/// Allow to perform eager operations on the session
	/// </summary>
	public interface IEagerSessionOperations
	{
		/// <summary>
		/// Execute all the lazy requests pending within this session
		/// </summary>
		void ExecuteAllPendingLazyOperations();
	}
}