using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Raven.Client.Exceptions;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Replication.Messages
{
    public class LiveTopologyInfo
    {
        public string DatabaseId;

        public Dictionary<string, NodeTopologyInfo> NodesById = new Dictionary<string, NodeTopologyInfo>();

        public List<InactiveNodeStatus> FailedToReach = new List<InactiveNodeStatus>();

        public DynamicJsonValue ToJson()
        {
            var nodesByDbIdJson = new DynamicJsonValue();
            foreach (var kvp in NodesById)
                nodesByDbIdJson[kvp.Key] = kvp.Value.ToJson();

            var failedToReachJson = new DynamicJsonArray();
            foreach (var kvp in FailedToReach)
                failedToReachJson.Add(kvp.ToJson());

            return new DynamicJsonValue
            {
                [nameof(DatabaseId)] = DatabaseId,
                [nameof(NodesById)] = nodesByDbIdJson,
                [nameof(FailedToReach)] = failedToReachJson
            };
        }
    }

    public class NodeTopologyInfo
    {
        public string DatabaseId;

        public Architecture ProcessArchitecture;

        public Architecture OSArchitecture;

        public string OSPlatformAsString;

        public OSPlatform OSPlatform => OSPlatform.Create(OSPlatformAsString);

        public string OSDescription;

        public string OSType;

        public List<ActiveNodeStatus> Outgoing;
        public List<ActiveNodeStatus> Incoming;
        public List<InactiveNodeStatus> Offline;

        public void InitializeOSInformation()
        {
            ProcessArchitecture = RuntimeInformation.ProcessArchitecture;
            OSArchitecture = RuntimeInformation.OSArchitecture;
            OSPlatformAsString = GetOSPlatform().ToString();
            OSDescription = RuntimeInformation.OSDescription; //should be OS name and version
        }

        public NodeTopologyInfo()
        {
            Outgoing = new List<ActiveNodeStatus>();
            Incoming = new List<ActiveNodeStatus>();
            Offline = new List<InactiveNodeStatus>();
        }


        public DynamicJsonValue ToJson()
        {
            var outgoingJson = new DynamicJsonArray();
            foreach (var outgoing in Outgoing)
            {
                outgoingJson.Add(outgoing.ToJson());
            }

            var incomingJson = new DynamicJsonArray();
            foreach (var incoming in Incoming)
            {
                incomingJson.Add(incoming.ToJson());
            }

            var offlineJson = new DynamicJsonArray();
            foreach (var offline in Offline)
            {
                offlineJson.Add(offline.ToJson());
            }

            return new DynamicJsonValue
            {
                [nameof(DatabaseId)] = DatabaseId,
                [nameof(ProcessArchitecture)] = ProcessArchitecture.ToString(),
                [nameof(OSArchitecture)] = OSArchitecture.ToString(),
                [nameof(OSPlatformAsString)] = OSPlatformAsString,
                [nameof(OSDescription)] = OSDescription,
                [nameof(Outgoing)] = outgoingJson,
                [nameof(Incoming)] = incomingJson,
                [nameof(Offline)] = offlineJson
            };
        }

        private static OSPlatform GetOSPlatform()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return OSPlatform.Windows;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                return OSPlatform.Linux;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return OSPlatform.OSX;

            throw new NotSupportedOSException();
        }

    }

    public class InactiveNodeStatus
    {
        public string Exception;

        public string Url;

        public string Message;

        public string Database;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Exception)] = Exception,
                [nameof(Message)] = Message,
                [nameof(Url)] = Url,
                [nameof(Database)] = Database
            };
        }
    }

    public class ActiveNodeStatus
    {
        public string DbId;

        public bool IsCurrentlyConnected;

        public long LastDocumentEtag;

        public long LastHeartbeatTicks;

        public string LastException;

        public string Url;

        public string Database;

        public Dictionary<string, string> SpecifiedCollections;

        public bool IsETLNode => SpecifiedCollections != null && SpecifiedCollections.Count > 0;

        public Status NodeStatus;

        public enum Status
        {
            Online,
            Timeout,
            Error
        }

        public ActiveNodeStatus()
        {
            SpecifiedCollections = new Dictionary<string, string>();
        }

        public DynamicJsonValue ToJson()
        {
            var specifiedCollectionsJson = new DynamicJsonValue();
            foreach (var key in SpecifiedCollections.Keys)
            {
                specifiedCollectionsJson[key] = SpecifiedCollections[key];
            }

            return new DynamicJsonValue
            {
                [nameof(IsCurrentlyConnected)] = IsCurrentlyConnected,
                [nameof(DbId)] = DbId,
                [nameof(Database)] = Database,
                [nameof(Url)] = Url,
                [nameof(LastException)] = LastException,
                [nameof(LastHeartbeatTicks)] = LastHeartbeatTicks,
                [nameof(LastDocumentEtag)] = LastDocumentEtag,
                [nameof(NodeStatus)] = NodeStatus.ToString(),
                [nameof(SpecifiedCollections)] = specifiedCollectionsJson,
                ["LastHeartbeat"] = new DateTime(LastHeartbeatTicks).GetDefaultRavenFormat(),
            };
        }
    }

}