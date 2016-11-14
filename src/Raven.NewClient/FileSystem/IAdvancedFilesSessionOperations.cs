using Raven.NewClient.Abstractions.Exceptions;
using Raven.NewClient.Client.Exceptions;

namespace Raven.NewClient.Client.FileSystem
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

        /// <summary>
        /// Gets or sets a value indicating whether the session should use optimistic concurrency.
        /// When set to <c>true</c>, a check is made so that a change made behind the session back would fail
        /// and raise <see cref="ConcurrencyException"/>.
        /// </summary>
        /// <value></value>
        bool UseOptimisticConcurrency { get; set; }
    }
}
