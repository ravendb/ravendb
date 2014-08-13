using Raven.Abstractions.FileSystem;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IFilesQuery<T> : IEnumerable<T>, IFilesQueryBase<T, IFilesQuery<T>>
    {
        /// <summary>
        /// Gets the query result
        /// Execute the query the first time that this is called.
        /// </summary>
        /// <value>The query result.</value>
        SearchResults QueryResult { get; }
        bool IsDistinct { get; }

        IFilesQuery<T> OnDirectory(string path, bool recursive = false);
    }
}
