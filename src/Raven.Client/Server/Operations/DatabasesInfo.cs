﻿using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.Operations
{
    public class DatabasesInfo
    {
        public List<DatabaseInfo> Databases { get; set; }
    }

    public class BackupInfo : IDynamicJson
    {
        public DateTime? LastBackup { get; set; }

        public int IntervalUntilNextBackupInSec { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastBackup)] = LastBackup,
                [nameof(IntervalUntilNextBackupInSec)] = IntervalUntilNextBackupInSec
            };
        }
    }

    public class DatabaseInfo : IDynamicJson
    {
        public string Name { get; set; }
        public bool Disabled { get; set; }
        public Size TotalSize { get; set; }

        public bool IsAdmin { get; set; }
        public TimeSpan? UpTime { get; set; }
        public BackupInfo BackupInfo { get; set; }

        public long? Alerts { get; set; }
        public bool RejectClients { get; set; }
        public string LoadError { get; set; }
        public long? IndexingErrors { get; set; }

        public long? DocumentsCount { get; set; }
        public bool HasVersioningConfiguration { get; set; }
        public bool HasExpirationConfiguration { get; set; }
        public int? IndexesCount { get; set; }
        public IndexRunningStatus IndexingStatus { get; set; }

        public NodesTopology NodesTopology { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Disabled)] = Disabled,
                [nameof(TotalSize)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = TotalSize.HumaneSize,
                    [nameof(Size.SizeInBytes)] = TotalSize.SizeInBytes
                },

                [nameof(IsAdmin)] = IsAdmin,
                [nameof(UpTime)] = UpTime?.ToString(),
                [nameof(BackupInfo)] = BackupInfo?.ToJson(),

                [nameof(Alerts)] = Alerts,
                [nameof(RejectClients)] = false,
                [nameof(IndexingErrors)] = IndexingErrors,

                [nameof(DocumentsCount)] = DocumentsCount,
                [nameof(HasVersioningConfiguration)] = HasVersioningConfiguration,
                [nameof(HasExpirationConfiguration)] = HasExpirationConfiguration,
                [nameof(IndexesCount)] = IndexesCount,
                [nameof(IndexingStatus)] = IndexingStatus.ToString(),

                [nameof(NodesTopology)] = NodesTopology?.ToJson()
            };
        }
    }

    public class NodesTopology : IDynamicJson
    {
        public List<NodeId> Members { get; set; }
        public List<NodeId> Promotables { get; set; }

        public NodesTopology()
        {
            Members = new List<NodeId>();
            Promotables = new List<NodeId>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Members)] = new DynamicJsonArray(Members.Select(x => x.ToJson())),
                [nameof(Promotables)] = new DynamicJsonArray(Promotables.Select(x => x.ToJson())),
            };
        }
    }

    public class NodeId : IDynamicJson
    {
        public string NodeTag;
        public string NodeUrl;
        public string ResponsibleNode;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(NodeUrl)] = NodeUrl,
                [nameof(ResponsibleNode)] = ResponsibleNode,
            };
        }
    }
}