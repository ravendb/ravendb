using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem.Listeners
{
    public interface IFilesWriteListener
    {
        /// <summary>
        /// Invoked before the written data is sent to the server.
        /// </summary>
        /// <param name="file">The file.</param>
        /// <param name="data">The data stream to send.</param>
        /// <param name="metadata">The metadata.</param>
        /// <returns>
        /// </returns>
        RavenJObject BeforeWrite(FileHeader file, Stream data, RavenJObject metadata);

        /// <summary>
        /// Invoked after the data was sent to the server.
        /// </summary>
        /// <param name="file">The updated file information.</param>
        /// <param name="metadata">The current metadata as stored in the server.</param>
        void AfterWrite(FileHeader file, RavenJObject metadata);
    }
}
