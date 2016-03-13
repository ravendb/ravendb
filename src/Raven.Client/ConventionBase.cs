using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;

namespace Raven.Client
{
    public abstract class ConventionBase
    {
        /// <summary>
        /// Enable multiple async operations
        /// </summary>
        public bool AllowMultipleAsyncOperations { get; set; }

        /// <summary>
        /// Whatever or not RavenDB should cache the request to the specified url.
        /// </summary>
        public Func<string, bool> ShouldCacheRequest { get; set; }

        /// <summary>
        /// How should we behave in a replicated environment when we can't 
        /// reach the primary node and need to failover to secondary node(s).
        /// </summary>
        public FailoverBehavior FailoverBehavior { get; set; }

        public FailoverBehavior FailoverBehaviorWithoutFlags
        {
            get { return FailoverBehavior & (~FailoverBehavior.ReadFromAllServers); }
        }

        public double RequestTimeThresholdInMilliseconds { get; set; }

        public string AuthenticationScheme { get; set; }

        /// <summary>
        /// Begins handling of unauthenticated responses, usually by authenticating against the oauth server
        /// in async manner
        /// </summary>
        public Func<HttpResponseMessage, OperationCredentials, Task<Action<HttpClient>>> HandleUnauthorizedResponseAsync { get; set; }

        /// <summary>
        /// Begins handling of forbidden responses
        /// in async manner
        /// </summary>
        public Func<HttpResponseMessage, OperationCredentials, Task<Action<HttpClient>>> HandleForbiddenResponseAsync { get; set; }
    }
}
