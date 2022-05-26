using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class DatabasesInfo : AbstractDashboardNotification
    {
        public List<DatabaseInfoItem> Items { get; set; }

        public DatabasesInfo()
        {
            Items = new List<DatabaseInfoItem>();
        }
    }

    public class DatabaseInfoItem : IDynamicJson
    {
        public string Database { get; set; }

        public long DocumentsCount { get; set; }

        public long IndexesCount { get; set; }

        public long ErroredIndexesCount { get; set; }

        public long AlertsCount { get; set; }

        public long PerformanceHintsCount { get; set; }

        public int ReplicationFactor { get; set; }

        public bool Online { get; set; }

        public bool Disabled { get; set; }

        public bool Irrelevant { get; set; }

        public long IndexingErrorsCount { get; set; }

        public BackupInfo BackupInfo { get; set; }
        
        public long OngoingTasksCount { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(DocumentsCount)] = DocumentsCount,
                [nameof(IndexesCount)] = IndexesCount,
                [nameof(ErroredIndexesCount)] = ErroredIndexesCount,
                [nameof(IndexingErrorsCount)] = IndexingErrorsCount,
                [nameof(AlertsCount)] = AlertsCount,
                [nameof(PerformanceHintsCount)] = PerformanceHintsCount,
                [nameof(ReplicationFactor)] = ReplicationFactor,
                [nameof(BackupInfo)] = BackupInfo?.ToJson(),
                [nameof(Online)] = Online,
                [nameof(Disabled)] = Disabled,
                [nameof(Irrelevant)] = Irrelevant,
                [nameof(OngoingTasksCount)] = OngoingTasksCount
            };
        }
    }
}
