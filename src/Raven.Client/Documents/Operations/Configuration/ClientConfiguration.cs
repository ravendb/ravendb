using System;
using Raven.Client.Http;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Configuration
{
    /// <summary>
    /// <para>Represents the client configuration settings that control how the client communicates with the server.</para>
    /// <para>This class includes options such as request limits, load balancing behaviors, and identity parts separator.</para>
    /// </summary>
    public sealed class ClientConfiguration
    {
        private char? _identityPartsSeparator;

        /// <summary>
        /// A version identifier for the configuration.
        /// </summary>
        public long Etag { get; set; }

        /// <summary>
        /// Indicates whether the client configuration is disabled.
        /// </summary>
        public bool Disabled { get; set; }

        /// <summary>
        /// The maximum number of requests allowed per session.
        /// </summary>
        public int? MaxNumberOfRequestsPerSession { get; set; }

        /// <summary>
        /// Specifies the read balance behavior to be used by the client.
        /// </summary>
        public ReadBalanceBehavior? ReadBalanceBehavior { get; set; }

        /// <summary>
        /// Specifies the load balance behavior to be used by the client.
        /// </summary>
        public LoadBalanceBehavior? LoadBalanceBehavior { get; set; }

        /// <summary>
        /// A seed value used by the load balancer for distributing requests.
        /// </summary>
        public int? LoadBalancerContextSeed { get; set; }

        /// <summary>
        /// A character used to separate parts of an identity. Cannot be set to '|'.
        /// </summary>
        public char? IdentityPartsSeparator
        {
            get => _identityPartsSeparator;
            set
            {
                if (value == '|')
                    throw new InvalidOperationException("Cannot set identity parts separator to '|'.");

                _identityPartsSeparator = value;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(Etag)] = Etag,
                [nameof(MaxNumberOfRequestsPerSession)] = MaxNumberOfRequestsPerSession,
                [nameof(ReadBalanceBehavior)] = ReadBalanceBehavior,
                [nameof(LoadBalanceBehavior)] = LoadBalanceBehavior,
                [nameof(LoadBalancerContextSeed)] = LoadBalancerContextSeed,
                [nameof(IdentityPartsSeparator)] = IdentityPartsSeparator
            };
        }
    }
}
