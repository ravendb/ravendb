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
        bool IsDistinct { get; }

        IFilesQuery<T> OnDirectory(string path, bool recursive = false);
    }
}
