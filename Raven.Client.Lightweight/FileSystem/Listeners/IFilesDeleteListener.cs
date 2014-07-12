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
        bool BeforeDelete(FileHeader instance);

        /// <summary>
        /// Invoked after the delete operation was effective on the server.
        /// </summary>
        /// <param name="instance">The file to delete</param>
        void AfterDelete(FileHeader instance);
    }
}
