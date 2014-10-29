using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Raven.Client.Document.Batches
{
	/// <summary>
	/// Fluent interface for specifying include paths
	/// for loading documents lazily
	/// </summary>
	public interface ILazyLoaderWithInclude<T>
	{
		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		ILazyLoaderWithInclude<T> Include(string path);

		/// <summary>
		/// Includes the specified path.
		/// </summary>
		/// <param name="path">The path.</param>
		/// <returns></returns>
		ILazyLoaderWithInclude<T> Include(Expression<Func<T, object>> path);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Lazy<T[]> Load(params string[] ids);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Lazy<T[]> Load(IEnumerable<string> ids);

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		Lazy<T> Load(string id);

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
		Lazy<T> Load(ValueType id);

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
		Lazy<T[]> Load(params ValueType[] ids);

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
		Lazy<T[]> Load(IEnumerable<ValueType> ids);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Lazy<TResult[]> Load<TResult>(params string[] ids);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <returns></returns>
		Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids);

		/// <summary>
		/// Loads the specified id.
		/// </summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="id">The id.</param>
		/// <returns></returns>
		Lazy<TResult> Load<TResult>(string id);

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
		Lazy<TResult> Load<TResult>(ValueType id);

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
		Lazy<TResult[]> Load<TResult>(params ValueType[] ids);

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
		Lazy<TResult[]> Load<TResult>(IEnumerable<ValueType> ids);
	}
    public interface IAsyncLazyLoaderWithInclude<T>
    {
        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        IAsyncLazyLoaderWithInclude<T> Include(string path);

        /// <summary>
        /// Includes the specified path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        IAsyncLazyLoaderWithInclude<T> Include(Expression<Func<T, object>> path);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Lazy<Task<T[]>> LoadAsync(params string[] ids);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Lazy<Task<T[]>> LoadAsync(IEnumerable<string> ids);

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        Lazy<Task<T>> LoadAsync(string id);

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
        Lazy<Task<T>> LoadAsync(ValueType id);

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
        Lazy<Task<T[]>> LoadAsync(params ValueType[] ids);

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
        Lazy<Task<T[]>> LoadAsync(IEnumerable<ValueType> ids);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        Lazy<Task<TResult[]>> LoadAsync<TResult>(params string[] ids);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
       Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<string> ids);

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
       Lazy<Task<TResult>> LoadAsync<TResult>(string id);

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
       Lazy<Task<TResult>> LoadAsync<TResult>(ValueType id);

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
       Lazy<Task<TResult[]>> LoadAsync<TResult>(params ValueType[] ids);

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
       Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<ValueType> ids);
    }
}
