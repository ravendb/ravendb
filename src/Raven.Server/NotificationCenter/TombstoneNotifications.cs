using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter
{
    public class TombstoneNotifications
    {
        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;

        public TombstoneNotifications(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
        }

        public void Add(List<BlockingTombstoneDetails> blockingTombstones)
        {
            var details = new BlockingTombstonesDetails(blockingTombstones);
            _notificationCenter.Add(AlertRaised.Create(
                _database, 
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
            using (_notificationsStorage.Read(id, out var value))
            {
                if (value == null ||
                    value.Json.TryGet(nameof(AlertRaised.Details), out BlittableJsonReaderObject details) == false ||
                    details.TryGet(nameof(BlockingTombstonesDetails.BlockingTombstones), out BlittableJsonReaderArray blockingTombstonesDetails) == false) 
                    return list;

                foreach (BlittableJsonReaderObject detail in blockingTombstonesDetails)
                {
                    detail.TryGet(nameof(BlockingTombstoneDetails.Source), out string source);
                    detail.TryGet(nameof(BlockingTombstoneDetails.Collection), out string collection);
                    detail.TryGet(nameof(BlockingTombstoneDetails.NumberOfTombstones), out long numOfTombstones);

                    list.Add(new BlockingTombstoneDetails
                    {
                        Source = source,
                        Collection = collection,
                        NumberOfTombstones = numOfTombstones
                    });
                }
            }

            return list;
        }

        internal class BlockingTombstonesDetails : INotificationDetails
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
                    jsonArray.Add( new DynamicJsonValue
                    {
                        [nameof(BlockingTombstoneDetails.Source)] = tombstoneDetails.Source,
                        [nameof(BlockingTombstoneDetails.Collection)] = tombstoneDetails.Collection,
                        [nameof(BlockingTombstoneDetails.NumberOfTombstones)] = tombstoneDetails.NumberOfTombstones
                    });
                }

                return new DynamicJsonValue
                {
                    [nameof(BlockingTombstones)] = jsonArray
                };
            }
        }

        public class BlockingTombstoneDetails
        {
            public string Source { get; set; }
            public string Collection { get; set; }
            public long NumberOfTombstones { get; set; }
        }
    }
}

