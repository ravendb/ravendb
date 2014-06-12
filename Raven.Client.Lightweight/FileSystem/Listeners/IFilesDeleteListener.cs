using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Listeners
{
    public interface IFilesDeleteListener
    {
        /// <summary>
        /// Invoked before the delete request is sent to the server.
        /// </summary>
        /// <param name="instance">The file to delete</param>
        /// <param name="metadata">The metadata</param>
        void BeforeDelete(FileHeader instance, RavenJObject metadata);

        /// <summary>
        /// Invoked before the delete request is sent to the server.
        /// </summary>
        /// <param name="instance">The directory to delete</param>
        /// <param name="metadata">The metadata</param>
        void BeforeDelete(DirectoryHeader instance, RavenJObject metadata);
    }
}
