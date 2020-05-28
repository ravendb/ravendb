using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class PullReplicationDefinition : IDynamicJsonValueConvertible
    {
        public Dictionary<string, string> Certificates; // <thumbprint, base64 cert>

        public TimeSpan DelayReplicationFor;
        public bool Disabled;

        public Dictionary<string, FilteringOptions> Filters;
        public string MentorNode;

        public string Name;
        public long TaskId;

        public ReplicationMode Mode = ReplicationMode.Pull;

        public PullReplicationDefinition() { }

        public PullReplicationDefinition(string name, TimeSpan delay = default, string mentor = null)
        {
            MentorNode = mentor;
            DelayReplicationFor = delay;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Certificates)] = DynamicJsonValue.Convert(Certificates),
                [nameof(TaskId)] = TaskId,
                [nameof(Disabled)] = Disabled,
                [nameof(MentorNode)] = MentorNode,
                [nameof(DelayReplicationFor)] = DelayReplicationFor,
                [nameof(Filters)] = DynamicJsonValue.Convert(Filters),
                [nameof(Mode)] = Mode,
            };
        }

        public ExternalReplication ToExternalReplication(ReplicationInitialRequest request, long taskId)
        {
            return new ExternalReplication
            {
                Url = request.SourceUrl,
                Database = request.Database,
                Name = request.PullReplicationSinkTaskName,
                DelayReplicationFor = DelayReplicationFor,
                MentorNode = MentorNode,
                TaskId = taskId
            };
        }


        public bool CanAccess(string thumbprint)
        {
            if (string.IsNullOrEmpty(thumbprint)) return false;

            if (Certificates?.ContainsKey(thumbprint) == true)
                return true; // we will authenticate the certificate later on the tcp level.

            return false;
        }

        public void Validate(bool useSsl)
        {
            if (string.IsNullOrEmpty(Name))
                throw new ArgumentNullException(nameof(Name));

            if (useSsl == false)
            {
                if (Certificates?.Count > 0)
                    throw new InvalidOperationException("Your server is unsecured and therefore you can't define pull replication with a certificate.");

                return;
            }

            if (Certificates == null)
                return;

            if ((Filters?.Count ?? 0) == 0)
                return;

            if (Filters.Count != Certificates.Count)
                throw new InvalidOperationException(
                    "When using filtered replication, each certificate must have its own filter configuration, " +
                    $"but found: {Certificates.Count:#,#;;0} certificates and only {Filters.Count:#,#;;0} filters");

            foreach (var kvp in Certificates.Where(kvp => Filters.ContainsKey(kvp.Key) == false))
            {
                throw new InvalidOperationException(
                    "When using filtered replication, each certificate must have its own filter configuration, " +
                    $"but there is no filter defined for certificate: {kvp.Key}");
            }

            foreach (var kvp in Filters)
            {
                if ((kvp.Value?.AllowedPaths?.Length ?? 0) == 0)
                    throw new InvalidOperationException($"Filter for {kvp} has a null or empty filter definition");

                foreach (string path in kvp.Value.AllowedPaths)
                {
                    if (string.IsNullOrEmpty(path))
                        throw new InvalidOperationException("Filtered replication AllowedPaths cannot have an empty / null filter");

                    if (path[path.Length - 1] != '*')
                        continue;

                    if (path.Length > 1 && path[path.Length - 2] != '/' && path[path.Length - 2] != '-')
                        throw new InvalidOperationException(
                            $"When using '*' at the end of the allowed path, the previous character must be '/' or '-', but got: {path} for {kvp.Key}");
                }
            }
        }

        public class FilteringOptions : IDynamicJson
        {
            public string[] AllowedPaths;
            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(AllowedPaths)] = AllowedPaths
                };
            }
        }
    }
}
