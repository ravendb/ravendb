using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IAsyncFilesOrderedQuery<T> : IAsyncFilesQuery<T>, IAsyncFilesOrderedQueryBase<T, IAsyncFilesQuery<T>>
    {
    }

    public interface IAsyncFilesQuery<T> : IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>
    {
        bool IsDistinct { get; }

        IAsyncFilesQuery<T> OnDirectory(string path = null, bool recursive = false);

        Task<List<T>> ToListAsync();

        /// <summary>
        ///   Deletes the files matching the query.
        /// </summary>
        void RegisterResultsForDeletion();

        FilesQuery GetFilesQuery();
    }
}
