using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter
{
    public sealed class TombstoneNotifications
    {
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;


        public TombstoneNotifications(AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter;
        }

        public void Add(List<BlockingTombstoneDetails> blockingTombstones)
        {
            var details = new BlockingTombstonesDetails(blockingTombstones);
            _notificationCenter.Add(AlertRaised.Create(
                _notificationCenter.Database,
                title: "Blockage in tombstone deletion",
                msg: "We have detected a blockage in tombstone deletion due to certain processes being in the disabled, errored, or paused states. Deletion or enabling of certain processes may be required.",
                type: AlertType.BlockingTombstones,
                severity: NotificationSeverity.Warning,
                key: nameof(AlertType.BlockingTombstones),
                details));
        }

        public List<BlockingTombstoneDetails> GetNotificationDetails(string id)
        {
            var list = new List<BlockingTombstoneDetails>();
            using (_notificationCenter.Storage.Read(id, out var notification))
            {
                using (notification)
                {
                    if (notification == null ||
                        notification.Json.TryGet(nameof(AlertRaised.Details), out BlittableJsonReaderObject details) == false ||
                        details.TryGet(nameof(BlockingTombstonesDetails.BlockingTombstones), out BlittableJsonReaderArray blockingTombstonesDetails) == false)
                        return list;

                    list.AddRange(
                        from BlittableJsonReaderObject detail in blockingTombstonesDetails
                        select JsonDeserializationServer.BlockingTombstoneDetails(detail));
                }
            }

            return list;
        }

        internal sealed class BlockingTombstonesDetails : INotificationDetails
        {
            internal List<BlockingTombstoneDetails> BlockingTombstones { get; set; }

            public BlockingTombstonesDetails(List<BlockingTombstoneDetails> blockingTombstones)
            {
                BlockingTombstones = blockingTombstones;
            }

            public DynamicJsonValue ToJson()
            {
                var jsonArray = new DynamicJsonArray();
                foreach (var tombstoneDetails in BlockingTombstones)
                {
                    jsonArray.Add(new DynamicJsonValue
                    {
                        [nameof(BlockingTombstoneDetails.Source)] = tombstoneDetails.Source,
                        [nameof(BlockingTombstoneDetails.BlockerType)] = tombstoneDetails.BlockerType,
                        [nameof(BlockingTombstoneDetails.BlockerTaskId)] = tombstoneDetails.BlockerTaskId,
                        [nameof(BlockingTombstoneDetails.Collection)] = tombstoneDetails.Collection,
                        [nameof(BlockingTombstoneDetails.NumberOfTombstones)] = tombstoneDetails.NumberOfTombstones,
                        [nameof(BlockingTombstoneDetails.SizeOfTombstonesInBytes)] = tombstoneDetails.SizeOfTombstonesInBytes
                    });
                }

                return new DynamicJsonValue
                {
                    [nameof(BlockingTombstones)] = jsonArray
                };
            }
        }
    }

    public sealed class BlockingTombstoneDetails
    {
        public string Source { get; set; }
        public ITombstoneAware.TombstoneDeletionBlockerType BlockerType { get; set; }
        public long BlockerTaskId { get; set; }
        public string Collection { get; set; }
        public long NumberOfTombstones { get; set; }
        public long SizeOfTombstonesInBytes { get; set; }
        public string SizeOfTombstonesHumane => new Size(SizeOfTombstonesInBytes, SizeUnit.Bytes).ToString();
    }

    public sealed class TombstoneDeletionBlockageSource : IEquatable<TombstoneDeletionBlockageSource>
    {
        public long TaskId { get; }
        public ITombstoneAware.TombstoneDeletionBlockerType Type { get; }
        public string Name { get; }

        public TombstoneDeletionBlockageSource(ITombstoneAware.TombstoneDeletionBlockerType blockerType, string name = null, long taskId = 0)
        {
            TaskId = taskId;
            Type = blockerType;
            Name = name;
        }

        public bool Equals(TombstoneDeletionBlockageSource other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;
            return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase)
                   && Type == other.Type
                   && TaskId == other.TaskId;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            return obj.GetType() == GetType() && Equals((TombstoneDeletionBlockageSource)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name?.ToLowerInvariant(), (int)Type, TaskId);
        }
    }
}

