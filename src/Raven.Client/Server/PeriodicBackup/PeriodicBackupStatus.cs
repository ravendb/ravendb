using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.PeriodicBackup
{
    public class PeriodicBackupStatus
    {
        public long TaskId { get; set; }

        public BackupType BackupType { get; set; }

        public string NodeTag { get; set; }

        public LocalBackup LocalBackup { get; set; }

        public UploadToS3 UploadToS3 { get; set; }

        public UploadToGlacier UploadToGlacier { get; set; }

        public UploadToAzure UploadToAzure { get; set; }

        public DateTime? LastFullBackup
        {
            get
            {
                return GetLastBackup(allDateTimeTicks =>
                {
                    if (LocalBackup?.LastFullBackup != null)
                        allDateTimeTicks.Add(LocalBackup.LastFullBackup.Value.Ticks);

                    if (UploadToS3?.LastFullBackup != null)
                        allDateTimeTicks.Add(UploadToS3.LastFullBackup.Value.Ticks);

                    if (UploadToGlacier?.LastFullBackup != null)
                        allDateTimeTicks.Add(UploadToGlacier.LastFullBackup.Value.Ticks);

                    if (UploadToAzure?.LastFullBackup != null)
                        allDateTimeTicks.Add(UploadToAzure.LastFullBackup.Value.Ticks);
                });
            }
        }

        public DateTime? LastIncrementalBackup
        {
            get
            {
                return GetLastBackup(allDateTimeTicks =>
                {
                    if (LocalBackup?.LastIncrementalBackup != null)
                        allDateTimeTicks.Add(LocalBackup.LastIncrementalBackup.Value.Ticks);

                    if (UploadToS3?.LastIncrementalBackup != null)
                        allDateTimeTicks.Add(UploadToS3.LastIncrementalBackup.Value.Ticks);

                    if (UploadToGlacier?.LastIncrementalBackup != null)
                        allDateTimeTicks.Add(UploadToGlacier.LastIncrementalBackup.Value.Ticks);

                    if (UploadToAzure?.LastIncrementalBackup != null)
                        allDateTimeTicks.Add(UploadToAzure.LastIncrementalBackup.Value.Ticks);
                });
            }
        }

        private static DateTime? GetLastBackup(Action<List<long>> updateLastBackups)
        {
            var allLastBackupDateTimeTicks = new List<long>();
            updateLastBackups(allLastBackupDateTimeTicks);

            var minDate = allLastBackupDateTimeTicks.Count == 0 ?
                DateTime.MinValue :
                new DateTime(allLastBackupDateTimeTicks.Min());
            return minDate == DateTime.MinValue ? (DateTime?)null : minDate;
        }

        public long? LastEtag { get; set; }

        public long? DurationInMs { get; set; }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue();
            UpdateJson(json);
            return json;
        }

        public void UpdateJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TaskId;
            json[nameof(BackupType)] = BackupType;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(LocalBackup)] = LocalBackup?.ToJson();
            json[nameof(UploadToS3)] = UploadToS3?.ToJson();
            json[nameof(UploadToGlacier)] = UploadToGlacier?.ToJson();
            json[nameof(UploadToAzure)] = UploadToAzure?.ToJson();
            json[nameof(LastEtag)] = LastEtag;
            json[nameof(DurationInMs)] = DurationInMs;
        }

        public static string GenerateItemName(string databaseName, long taskId)
        {
            return $"periodic-backups/{databaseName}/{taskId}";
        }
    }
}