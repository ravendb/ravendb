using Raven.Abstractions.Data;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IAdvancedFilesSessionOperations
    {
        /// <summary>
        /// The filesystem store associated with this session
        /// </summary>
        IFilesStore FilesStore { get; }

        /// <summary>
        /// Gets the store identifier for this session.
        /// The store identifier is the identifier for the particular RavenDB instance. 
        /// </summary>
        /// <value>The store identifier.</value>
        string StoreIdentifier { get; }

        /// <summary>
        /// Gets or sets the max number of requests per session.
        /// If the <see cref="NumberOfRequests"/> rise above <see cref="MaxNumberOfRequestsPerSession"/>, an exception will be thrown.
        /// </summary>
        /// <value>The max number of requests per session.</value>
        int MaxNumberOfRequestsPerSession { get; set; }

        /// <summary>
        /// Gets the number of requests for this session
        /// </summary>
        int NumberOfRequests { get; }
    }
}
