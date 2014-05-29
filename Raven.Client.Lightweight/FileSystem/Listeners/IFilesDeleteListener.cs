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
        /// <param name="instance">The object to delete</param>
        /// <param name="metadata">The object metadata</param>
        void BeforeDelete(IRemoteObject instance, RavenJObject metadata);
    }
}
