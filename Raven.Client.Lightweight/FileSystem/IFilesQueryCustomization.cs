using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem
{
    public interface IFilesQueryCustomization
    {
        /// <summary>
        /// Disables tracking for queried entities by Raven's Unit of Work.
        /// Usage of this option will prevent holding query results in memory.
        /// </summary>
        IFilesQueryCustomization NoTracking();

        /// <summary>
        /// Disables caching for query results.
        /// </summary>
        IFilesQueryCustomization NoCaching();
    }
}
