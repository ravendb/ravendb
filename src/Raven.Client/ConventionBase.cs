using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Replication;

namespace Raven.Client
{
    public abstract class ConventionBase
    {
        /// <summary>
        /// Enable multiple async operations
        /// </summary>
        public bool AllowMultipleAsyncOperations { get; set; }

        /// <summary>
        /// How should we behave in a replicated environment when we can't 
        /// reach the primary node and need to failover to secondary node(s).
        /// </summary>
        public FailoverBehavior FailoverBehavior { get; set; }

        public FailoverBehavior FailoverBehaviorWithoutFlags => FailoverBehavior & (~FailoverBehavior.ReadFromAllServers);

        public double RequestTimeThresholdInMilliseconds { get; set; }

        public string AuthenticationScheme { get; set; }

        /// <summary>
        /// Begins handling of unauthenticated responses, usually by authenticating against the oauth server
        /// in async manner
        /// </summary>
        public Func<HttpResponseMessage, OperationCredentials, Task<Action<HttpClient>>> HandleUnauthorizedResponseAsync { get; set; }
    }
}
