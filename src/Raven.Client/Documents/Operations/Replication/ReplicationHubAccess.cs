using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{

    /// <summary>
    /// Represents the configuration for replication hub access.
    /// This class allows you to define the access permissions for replication between a hub and sink.
    /// </summary>
    public sealed class ReplicationHubAccess : IDynamicJson
    {
        /// <summary>
        /// The name of the replication hub access configuration.
        /// </summary>
        public string Name;
        /// <summary>
        /// The Base64-encoded certificate used to authenticate the access.
        /// </summary>
        public string CertificateBase64;
        /// <summary>
        /// An array of allowed paths for data replication from the hub to the sink.
        /// </summary>
        public string[] AllowedHubToSinkPaths;
        /// <summary>
        /// An array of allowed paths for data replication from the sink to the hub.
        /// </summary>
        public string[] AllowedSinkToHubPaths;

        public DynamicJsonValue ToJson()
        {
            PullReplicationPathFilterUtils.NormalizeAndValidate(ref AllowedHubToSinkPaths, ref AllowedSinkToHubPaths, Name);

            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(AllowedHubToSinkPaths)] = AllowedHubToSinkPaths,
                [nameof(AllowedSinkToHubPaths)] = AllowedSinkToHubPaths,
                [nameof(CertificateBase64)] = CertificateBase64
            };
        }

        internal void Validate(bool filteringIsRequired)
        {
            if (string.IsNullOrEmpty(Name))
                throw new ArgumentNullException(nameof(Name));

            PullReplicationPathFilterUtils.NormalizeAndValidate(ref AllowedHubToSinkPaths, ref AllowedSinkToHubPaths, Name);

            if (filteringIsRequired)
            {
                if ((AllowedHubToSinkPaths?.Length ?? 0) == 0 && (AllowedSinkToHubPaths?.Length ?? 0) == 0)
                    throw new InvalidOperationException(
                        $"Either {nameof(AllowedSinkToHubPaths)} or {nameof(AllowedHubToSinkPaths)} must have a value, but both were null or empty");
            }
            else
            {
                if (AllowedHubToSinkPaths?.Length > 0 || AllowedSinkToHubPaths?.Length > 0)
                    throw new InvalidOperationException(
                        $"Filtering replication is not set for this Replication Hub task." +
                        $" {nameof(AllowedSinkToHubPaths)} and {nameof(AllowedHubToSinkPaths)} cannot have a value.");
            }
        }

    }
}
