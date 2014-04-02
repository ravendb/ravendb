using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using System.Threading.Tasks;

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
		ILazyLoaderWithInclude<TResult> Include<TResult>(Expression<Func<TResult, object>> path);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		Lazy<TResult[]> Load<TResult>(params string[] ids);

		/// <summary>
		/// Loads the specified ids.
		/// </summary>
		Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids);

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
		Lazy<TResult> Load<TResult>(ValueType id, Action<TResult> onEval);

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
		Lazy<TResult[]> Load<TResult>(IEnumerable<ValueType> ids, Action<TResult[]> onEval);

		Lazy<TResult> Load<TTransformer, TResult>(string id) where TTransformer : AbstractTransformerCreationTask, new();

		Lazy<TResult[]> Load<TTransformer, TResult>(params string[] ids) where TTransformer : AbstractTransformerCreationTask, new();

		/// <summary>
		/// Load documents with the specified key prefix
		/// </summary>
		Lazy<TResult[]> LoadStartingWith<TResult>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null);

		Lazy<TResult[]> MoreLikeThis<TResult>(MoreLikeThisQuery query);
	}

    /// <summary>
    /// Specify interface for lazy async operation for the session
    /// </summary>
    public interface IAsyncLazySessionOperations
    {
        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        IAsyncLazyLoaderWithInclude<object> Include(string path);

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        IAsyncLazyLoaderWithInclude<TResult> Include<TResult>(Expression<Func<TResult, object>> path);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        Lazy<Task<TResult[]>> LoadAsync<TResult>(params string[] ids);

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<string> ids);

        /// <summary>
        /// Loads the specified ids and a function to call when it is evaluated
        /// </summary>
        Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<string> ids, Action<TResult[]> onEval);

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        Lazy<Task<TResult>> LoadAsync<TResult>(string id);

        /// <summary>
        /// Loads the specified id and a function to call when it is evaluated
        /// </summary>
        Lazy<Task<TResult>> LoadAsync<TResult>(string id, Action<TResult> onEval);

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
        Lazy<Task<TResult>> LoadAsync<TResult>(ValueType id, Action<TResult> onEval);

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
        Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<ValueType> ids, Action<TResult[]> onEval);

        Lazy<Task<TResult>> LoadAsync<TTransformer, TResult>(string id) where TTransformer : AbstractTransformerCreationTask, new();

        Lazy<Task<TResult[]>> LoadAsync<TTransformer, TResult>(params string[] ids) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        /// Load documents with the specified key prefix
        /// </summary>
        Lazy<Task<TResult[]>> LoadStartingWithAsync<TResult>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null);

        Lazy<Task<TResult[]>> MoreLikeThisAsync<TResult>(MoreLikeThisQuery query);
    }

	/// <summary>
	/// Allow to perform eager operations on the session
	/// </summary>
	public interface IEagerSessionOperations
	{
		/// <summary>
		/// Execute all the lazy requests pending within this session
		/// </summary>
		ResponseTimeInformation ExecuteAllPendingLazyOperations();
	}

    public interface IAsyncEagerSessionOperations
    {
        /// <summary>
        /// Execute all the lazy requests pending within this session
        /// </summary>
        Task<ResponseTimeInformation> ExecuteAllPendingLazyOperationsAsync();
    }
}
