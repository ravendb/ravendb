using System;

using Raven.NewClient.Abstractions.Replication;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.FileSystem.Connection;

namespace Raven.NewClient.Client.FileSystem
{
    /// <summary>
    /// The set of conventions used by the <see cref="FilesConvention"/> which allow the users to customize
    /// the way the Raven client API behaves
    /// </summary>
    public class FilesConvention : QueryConvention
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FilesConvention"/> class.
        /// </summary>
        public FilesConvention()
        {
            FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
            AllowMultipleAsyncOperations = true;
            IdentityPartsSeparator = "/";
            ShouldCacheRequest = url => true;
            MaxNumberOfRequestsPerSession = 30;
            ReplicationInformerFactory = (url, jsonRequestFactory) => new FilesReplicationInformer(this, jsonRequestFactory);
        }

        /// <summary>
        /// Gets or sets the default max number of requests per session.
        /// </summary>
        /// <value>The max number of requests per session.</value>
        public int MaxNumberOfRequestsPerSession { get; set; }

        /// <summary>
        /// Whether UseOptimisticConcurrency is set to true by default for all opened sessions
        /// </summary>
        public bool DefaultUseOptimisticConcurrency { get; set; }

        /// <summary>
        /// Clone the current conventions to a new instance
        /// </summary>
        public FilesConvention Clone()
        {
            return (FilesConvention)MemberwiseClone();
        }

        /// <summary>
        /// This is called to provide replication behavior for the client. You can customize 
        /// this to inject your own replication / failover logic.
        /// </summary>
        public Func<string, HttpJsonRequestFactory, IFilesReplicationInformer> ReplicationInformerFactory { get; set; }
    }
}
