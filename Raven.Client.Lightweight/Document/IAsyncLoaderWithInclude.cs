using System;
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
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		AsyncMultiLoaderWithInclude<T> Include(string path);

		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		AsyncMultiLoaderWithInclude<T> Include(Expression<Func<T, object>> path);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Task<T[]> LoadAsync(params string[] ids);

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		Task<T> Load(string id);

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
		Task<T> Load(ValueType id);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Task<TResult[]> Load<TResult>(params string[] ids);

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		Task<TResult> Load<TResult>(string id);

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
		Task<TResult> Load<TResult>(ValueType id);
	}
}