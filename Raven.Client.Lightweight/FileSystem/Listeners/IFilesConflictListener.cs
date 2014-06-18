using Raven.Abstractions.FileSystem;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem.Listeners
{
    public interface IFilesConflictListener
    {
        /// <summary>
        /// Invoked when a conflict has been detected over a file.
        /// </summary>
        /// <param name="instance">The file in conflict</param>
        /// <param name="sourceServerUri">The Destination Uri where the conflict appeared</param>
        /// <returns>A resolution strategy for this conflict</returns>
        ConflictResolutionStrategy ConflictDetected(FileHeader instance, String sourceServerUri);

        /// <summary>
        /// Invoked when a file conflict has been resolved.
        /// </summary>
        /// <param name="instance">The file with the resolved conflict</param>
        void ConflictResolved(FileHeader instance);
 
    }
}
