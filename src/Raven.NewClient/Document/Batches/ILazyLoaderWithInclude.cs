using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Document.Batches
{
    /// <summary>
    ///     Fluent interface for specifying include paths
    ///     for loading documents lazily
    /// </summary>
    public interface ILazyLoaderWithInclude<T>
    {
        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        ILazyLoaderWithInclude<T> Include(string path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        ILazyLoaderWithInclude<T> Include(Expression<Func<T, object>> path);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<T[]> Load(params string[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<T[]> Load(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        Lazy<T> Load(string id);

        /// <summary>
        ///     Loads the specified entity with the specified id after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<T> Load(ValueType id);

        /// <summary>
        ///     Loads the specified entities with the specified ids after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1, 2, 3)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1", "posts/2", "posts/3");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<T[]> Load(params ValueType[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1, 2, 3)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1", "posts/2", "posts/3");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<T[]> Load(IEnumerable<ValueType> ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<TResult[]> Load<TResult>(params string[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        Lazy<TResult> Load<TResult>(string id);

        /// <summary>
        ///     Loads the specified entity with the specified id after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<TResult> Load<TResult>(ValueType id);

        /// <summary>
        ///     Loads the specified entities with the specified ids after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1, 2, 3)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1", "posts/2", "posts/3");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<TResult[]> Load<TResult>(params ValueType[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1, 2, 3)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1", "posts/2", "posts/3");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<TResult[]> Load<TResult>(IEnumerable<ValueType> ids);
    }

    public interface IAsyncLazyLoaderWithInclude<T>
    {
        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLazyLoaderWithInclude<T> Include(string path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLazyLoaderWithInclude<T> Include(Expression<Func<T, object>> path);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Task<T[]>> LoadAsync(params string[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Task<T[]>> LoadAsync(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        Lazy<Task<T>> LoadAsync(string id);

        /// <summary>
        ///     Loads the specified entity with the specified id after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<Task<T>> LoadAsync(ValueType id);

        /// <summary>
        ///     Loads the specified entities with the specified ids after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1, 2, 3)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1", "posts/2", "posts/3");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<Task<T[]>> LoadAsync(params ValueType[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1, 2, 3)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1", "posts/2", "posts/3");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<Task<T[]>> LoadAsync(IEnumerable<ValueType> ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Task<TResult[]>> LoadAsync<TResult>(params string[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        Lazy<Task<TResult>> LoadAsync<TResult>(string id);

        /// <summary>
        ///     Loads the specified entity with the specified id after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<Task<TResult>> LoadAsync<TResult>(ValueType id);

        /// <summary>
        ///     Loads the specified entities with the specified ids after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1, 2, 3)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1", "posts/2", "posts/3");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<Task<TResult[]>> LoadAsync<TResult>(params ValueType[] ids);

        /// <summary>
        ///     Loads the specified entities with the specified ids after applying
        ///     conventions on the provided id to get the real document id.
        ///     <para>This method allows you to call:</para>
        ///     <para>Load&lt;Post&gt;(1, 2, 3)</para>
        ///     <para>And that call will internally be translated to </para>
        ///     <para>Load&lt;Post&gt;("posts/1", "posts/2", "posts/3");</para>
        ///     <para>Or whatever your conventions specify.</para>
        /// </summary>
        Lazy<Task<TResult[]>> LoadAsync<TResult>(IEnumerable<ValueType> ids);
    }
}
