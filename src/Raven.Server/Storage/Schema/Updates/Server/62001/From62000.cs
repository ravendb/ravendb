using System;
using System.IO;
using Raven.Server.Documents.Schemas;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From62000 : ISchemaUpdate
    {
        public int From => 62_000;
        public int To => 71_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        private static readonly TableSchema LegacyNotificationsSchema = new TableSchema();
        private static readonly TableSchema NewNotificationsSchema = new TableSchema();

        private const string OldNotificationsTableName = "Notifications";
        
        private const string TypePropertyName = "Type";
        private const string ReasonPropertyName = "Reason";
        private const string AlertTypePropertyName = "AlertType";
        private const string HintTypePropertyName = "HintType";

        private const long UnsupportedReason = -1;
        
        private static class LegacyNotificationsTable
        {
            public const int IdIndex = 0;
            public const int CreatedAtIndex = 1;
            public const int PostponedUntilIndex = 2;
            public const int JsonIndex = 3;
        }

        private static class NewNotificationsTable
        {
            public const int IdIndex = 0;
            public const int CreatedAtIndex = 1;
            public const int PostponedUntilIndex = 2;
            public const int JsonIndex = 3;
            public const int TypeIndex = 4;
            public const int ReasonIndex = 5;
        }

        static From62000()
        {
            Slice byCreatedAt;
            Slice byPostponedUntil;
            Slice byType;
            
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out byCreatedAt);
                Slice.From(ctx, "ByPostponedUntil", ByteStringType.Immutable, out byPostponedUntil);
                Slice.From(ctx, "ByType", ByteStringType.Immutable, out byType);
            }
            
            LegacyNotificationsSchema.DefineKey(new TableSchema.IndexDef
            {
                StartIndex = LegacyNotificationsTable.IdIndex,
                Count = 1
            });

            LegacyNotificationsSchema.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
            {
                StartIndex = LegacyNotificationsTable.CreatedAtIndex,
                Name = byCreatedAt
            });

            LegacyNotificationsSchema.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
            {
                StartIndex = LegacyNotificationsTable.PostponedUntilIndex,
                Name = byPostponedUntil
            });

            NewNotificationsSchema.DefineKey(new TableSchema.IndexDef
            {
                StartIndex = NewNotificationsTable.IdIndex,
                Count = 1
            });

            NewNotificationsSchema.DefineIndex(new TableSchema.IndexDef
            {
                StartIndex = NewNotificationsTable.CreatedAtIndex,
                Name = byCreatedAt
            });

            NewNotificationsSchema.DefineIndex(new TableSchema.IndexDef
            {
                StartIndex = NewNotificationsTable.PostponedUntilIndex,
                Name = byPostponedUntil
            });
            
            NewNotificationsSchema.DefineIndex(new TableSchema.IndexDef
            {
                StartIndex = NewNotificationsTable.TypeIndex,
                Name = byType
            });
        }
        
        private static string GetOldTableName(string resourceName = null)
        {
            return string.IsNullOrEmpty(resourceName)
                ? OldNotificationsTableName
                : $"{OldNotificationsTableName}.{resourceName.ToLowerInvariant()}";
        }
        
        private static string GetNewTableName(string resourceName = null)
        {
            return string.IsNullOrEmpty(resourceName)
                ? $"{OldNotificationsTableName}.Server"
                : $"{OldNotificationsTableName}.Database.{resourceName.ToLowerInvariant()}";
        }
        
        public bool Update(UpdateStep step)
        {
            var databaseNames = SchemaUpgradeExtensions.GetDatabases(step);

            foreach (var databaseName in databaseNames)
                ProcessDatabase(step, databaseName);
            
            ProcessServer(step);
            
            return true;
        }

        private static void ProcessDatabase(UpdateStep step, string databaseName)
        {
            var oldTableName = GetOldTableName(databaseName);
            var newTableName = GetNewTableName(databaseName);
            
            ProcessResource(step, oldTableName, newTableName);
        }

        private static void ProcessServer(UpdateStep step)
        {
            var oldTableName = GetOldTableName();
            var newTableName = GetNewTableName();
            
            ProcessResource(step, oldTableName, newTableName);
        }
        
        private static unsafe void ProcessResource(UpdateStep step, string oldTableName, string newTableName)
        {
            var readTable = step.ReadTx.OpenTable(LegacyNotificationsSchema, oldTableName);

            if (readTable == null)
                return;
            
            Notifications.NotificationsSchemaBase.Create(step.WriteTx, newTableName, 16);
            var writeTable = step.WriteTx.OpenTable(NewNotificationsSchema, newTableName);
            
            using (var jsonContext = JsonOperationContext.ShortTermSingleUse())
            {
                foreach (var existingNotification in readTable.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                {
                    var reader = existingNotification.Reader;
                    
                    var id = reader.Read(LegacyNotificationsTable.IdIndex, out var idSize);
                    var createdAt = reader.Read(LegacyNotificationsTable.CreatedAtIndex, out var createdAtSize);
                    var postponedUntil = reader.Read(LegacyNotificationsTable.PostponedUntilIndex, out var postponedUntilSize);
                    var jsonPtr = reader.Read(LegacyNotificationsTable.JsonIndex, out var jsonSize);

                    var jsonBlittable = new BlittableJsonReaderObject(jsonPtr, jsonSize, jsonContext);

                    if (jsonBlittable.TryGet(TypePropertyName, out LazyStringValue notificationTypeLsv) == false)
                        throw new InvalidDataException($"Couldn't find {TypePropertyName} property in notification json.");

                    if (Enum.TryParse<NotificationType>(notificationTypeLsv, out var notificationType) == false)
                        throw new InvalidDataException($"Unexpected {nameof(NotificationType)}: {notificationTypeLsv}");

                    LazyStringValue notificationReasonLsv;

                    jsonBlittable.Modifications = new DynamicJsonValue(jsonBlittable);
                    
                    var notificationTypeLongValue = Bits.SwapBytes((long)notificationType);
                    long notificationReasonLongValue;

                    switch (notificationType)
                    {
                        case NotificationType.AlertRaised:
                            jsonBlittable.TryGet(AlertTypePropertyName, out notificationReasonLsv);
                            jsonBlittable.Modifications.Remove(AlertTypePropertyName);
                            
                            if (Enum.TryParse<AlertReason>(notificationReasonLsv, out var alertReason) == false)
                                throw new InvalidDataException($"Unexpected {nameof(AlertReason)}: {alertReason}");
                            
                            jsonBlittable.Modifications[ReasonPropertyName] = (long)alertReason;
                            notificationReasonLongValue = (long)alertReason;
                            
                            break;
                        case NotificationType.PerformanceHint:
                            jsonBlittable.TryGet(HintTypePropertyName, out notificationReasonLsv);
                            jsonBlittable.Modifications.Remove(HintTypePropertyName);
                            
                            if (Enum.TryParse<PerformanceHintReason>(notificationReasonLsv, out var performanceHintReason) == false)
                                throw new InvalidDataException($"Unexpected {nameof(PerformanceHintReason)}: {performanceHintReason}");
                            
                            jsonBlittable.Modifications[ReasonPropertyName] = (long)performanceHintReason;
                            notificationReasonLongValue = (long)performanceHintReason;
                            
                            break;
                        default:
                            notificationReasonLongValue = UnsupportedReason;
                            break;
                    }
                    
                    notificationReasonLongValue = Bits.SwapBytes(notificationReasonLongValue);
                    
                    jsonBlittable.Modifications[TypePropertyName] = (int)notificationType;

                    using (_ = jsonBlittable)
                    {
                        jsonBlittable = jsonContext.ReadObject(jsonBlittable, null);
                        
                        using (writeTable.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(id, idSize);
                            tvb.Add(createdAt, createdAtSize);
                            tvb.Add(postponedUntil, postponedUntilSize);
                            tvb.Add(jsonBlittable.BasePointer, jsonBlittable.Size);
                            tvb.Add((byte*)&notificationTypeLongValue, sizeof(long));
                            tvb.Add((byte*)&notificationReasonLongValue, sizeof(long));

                            writeTable.Insert(tvb);
                        }
                    }
                    
                    jsonBlittable.Dispose();
                }
            }
            
            step.WriteTx.DeleteTable(oldTableName);
            step.WriteTx.Commit();
            step.RenewTransactions();
        }
    }
}
