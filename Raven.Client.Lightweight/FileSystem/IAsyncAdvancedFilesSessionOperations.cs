using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;

namespace Raven.Client.FileSystem
{
    public interface IAsyncAdvancedFilesSessionOperations : IAdvancedFilesSessionOperations
    {
        /// <summary>
        ///     Stream the results on the query to the client
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChangesasync() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        Task<IAsyncEnumerator<FileHeader>> StreamQuery(IAsyncFilesQuery<FileHeader> query);
    }
}
