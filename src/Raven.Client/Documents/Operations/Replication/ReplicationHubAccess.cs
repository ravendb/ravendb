using System;
using System.Collections;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class ReplicationHubAccess : IDynamicJson
    {
        public string Name;
        public string CertificateBas64;
        
        public string[] AllowedReadPaths;
        public string[] AllowedWritePaths;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(AllowedReadPaths)] = AllowedReadPaths,
                [nameof(AllowedWritePaths)] = AllowedWritePaths,
                [nameof(CertificateBas64)] = CertificateBas64,
                
            };
        }

        public void Validate()
        {
            if (string.IsNullOrEmpty(Name))
                throw new ArgumentNullException(nameof(Name));

            if((AllowedReadPaths?.Length ?? 0) == 0 && (AllowedWritePaths?.Length == 0))
                throw new InvalidOperationException($"Either {nameof(AllowedWritePaths)} or {nameof(AllowedReadPaths)} must have a value, but both were null or empty");

            ValidateAllowedPaths(AllowedReadPaths);
            ValidateAllowedPaths(AllowedWritePaths);
        }
        
        
        void ValidateAllowedPaths(string[] allowedPaths)
        {
            if ((allowedPaths?.Length ?? 0) == 0)
                return;

            foreach (string path in allowedPaths)
            {
                if (string.IsNullOrEmpty(path))
                    throw new InvalidOperationException("Filtered replication AllowedPaths cannot have an empty / null filter");

                if (path[path.Length - 1] != '*')
                    continue;

                if (path.Length > 1 && path[path.Length - 2] != '/' && path[path.Length - 2] != '-')
                    throw new InvalidOperationException(
                        $"When using '*' at the end of the allowed path, the previous character must be '/' or '-', but got: {path} for {Name}");
            }
        }
    }

    public class DetailedReplicationHubAccess
    {
        public string Name;
        public string Thumbprint;
        public DateTime NotBefore, NotAfter;
        public string Subject;
        public string Issuer;
        
        public string[] AllowedReadPaths;
        public string[] AllowedWritePaths;

    }

    public class ReplicationHubAccessList : IEnumerable<DetailedReplicationHubAccess>
    {
        public int Skip;
        public List<DetailedReplicationHubAccess> Results = new List<DetailedReplicationHubAccess>();
        
        public IEnumerator<DetailedReplicationHubAccess> GetEnumerator()
        {
            return Results.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
    
    public class ListReplicationHubAccessOperation : IMaintenanceOperation<ReplicationHubAccessList>
    {
        private readonly string _hubDefinitionName;
        private readonly int _start;
        private readonly int _pageSize;

        public ListReplicationHubAccessOperation(string hubDefinitionName, int start = 0, int pageSize = 25)
        {
            _hubDefinitionName = hubDefinitionName;
            _start = start;
            _pageSize = pageSize;
        }
        
        public RavenCommand<ReplicationHubAccessList> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ListReplicationHubAccessCommand(_hubDefinitionName, _start, _pageSize); 
        }

        public class ListReplicationHubAccessCommand : RavenCommand<ReplicationHubAccessList>
        {
            private readonly string _hubDefinitionName;
            private readonly int _start;
            private readonly int _pageSize;

            public ListReplicationHubAccessCommand(string hubDefinitionName, int start, int pageSize)
            {
                _hubDefinitionName = hubDefinitionName;
                _start = start;
                _pageSize = pageSize;
            }

            public override bool IsReadRequest { get; } = true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/pull-replication/hub/access?hub={Uri.EscapeUriString(_hubDefinitionName)}&start={_start}&pageSize={_pageSize}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };

                return request;
            }
            
            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ReplicationHubAccessList(response);
            }
        }
    }
    
    public class RegisterReplicationHubAccessOperation : IMaintenanceOperation
    {
        private readonly string _hubDefinitionName;
        private readonly ReplicationHubAccess _access;

        public RegisterReplicationHubAccessOperation(string hubDefinitionName, ReplicationHubAccess access)
        {
            _hubDefinitionName = hubDefinitionName;
            _access = access;
        }
        
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RegisterReplicationHubAccessCommand(_hubDefinitionName, _access); 
        }

        public class RegisterReplicationHubAccessCommand : RavenCommand, IRaftCommand
        {
            private readonly string _hubDefinitionName;
            private readonly ReplicationHubAccess _access;

            public RegisterReplicationHubAccessCommand(string hubDefinitionName, ReplicationHubAccess access)
            {
                _hubDefinitionName = hubDefinitionName;
                _access = access;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/pull-replication/hub/access?hub={Uri.EscapeUriString(_hubDefinitionName)}";

                var blittable = ctx.ReadObject(_access.ToJson(), "register-access");

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        blittable.WriteJsonTo(stream);
                    })
                };

                return request;
            }

            public string RaftUniqueRequestId { get; } =  RaftIdGenerator.NewId();
        }
    }
    
    public class UnregisterReplicationHubAccessOperation : IMaintenanceOperation
    {
        private readonly string _hubDefinitionName;
        private readonly string _thumbprint;

        public UnregisterReplicationHubAccessOperation(string hubDefinitionName, string thumbprint)
        {
            _hubDefinitionName = hubDefinitionName;
            _thumbprint = thumbprint;
        }
        
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UnregisterReplicationHubAccessCommand(_hubDefinitionName, _thumbprint); 
        }

        public class UnregisterReplicationHubAccessCommand : RavenCommand, IRaftCommand
        {
            private readonly string _hubDefinitionName;
            private readonly string _thumbprint;

            public UnregisterReplicationHubAccessCommand(string hubDefinitionName, string thumbprint)
            {
                _hubDefinitionName = hubDefinitionName;
                _thumbprint = thumbprint;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/pull-replication/hub/access?hub={Uri.EscapeUriString(_hubDefinitionName)}&thumbprint={Uri.EscapeUriString(_thumbprint)}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
                };

                return request;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
