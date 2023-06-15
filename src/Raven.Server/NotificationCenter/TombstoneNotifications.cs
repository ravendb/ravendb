using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter
{
    public class TombstoneNotifications
    {
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;
        private const string _title = "Blocking of tombstones deletion";
        private const string _msg = $"We have detected blocking of tombstones deletion. Consider deleting or enabling the following processes:";

        public TombstoneNotifications(AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter;
        }

        public void Add(Dictionary<(string Source, string Collection), long> blockingTombstones)
        {
            BlockingTombstonesDetails details = new BlockingTombstonesDetails(blockingTombstones);
            _notificationCenter.Add(AlertRaised.Create(_notificationCenter.Database, _title, _msg, AlertType.BlockingTombstones,
                NotificationSeverity.Warning,
                nameof(AlertType.BlockingTombstones), details: details));
        }

        public List<BlockingTombstoneDetails> GetNotificationDetails(string id)
        {
            var list = new List<BlockingTombstoneDetails>();
            using (_notificationCenter.Storage.Read(id, out var value))
            {
                value.Json.TryGet(nameof(AlertRaised.Details), out BlittableJsonReaderObject details);
                details.TryGet(nameof(BlockingTombstonesDetails.BlockingTombstones), out BlittableJsonReaderArray blockingTombstonesDetails);

                foreach (BlittableJsonReaderObject detail in blockingTombstonesDetails)
                {
                    detail.TryGet(nameof(BlockingTombstoneDetails.Source), out string source);
                    detail.TryGet(nameof(BlockingTombstoneDetails.Collection), out string collection);
                    detail.TryGet(nameof(BlockingTombstoneDetails.NumberOfTombstones), out long numOfTombstones);
                    var blockingTombstoneDetails = new BlockingTombstoneDetails()
                    {
                        Source = source,
                        Collection = collection,
                        NumberOfTombstones = numOfTombstones
                    };
                    list.Add(blockingTombstoneDetails);
                }
            }

            return list;
        }

        internal class BlockingTombstonesDetails : INotificationDetails
        {
            public BlockingTombstonesDetails(Dictionary<(string Source, string Collection), long> blockingTombstones)
            {
                BlockingTombstones = blockingTombstones;
            }

            public Dictionary<(string Source, string Collection), long> BlockingTombstones { get; set; }

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonArray();
                foreach (var key in BlockingTombstones.Keys)
                {
                    djv.Add(new DynamicJsonValue
                    {
                        [nameof(BlockingTombstoneDetails.Source)] = key.Source,
                        [nameof(BlockingTombstoneDetails.Collection)] = key.Collection,
                        [nameof(BlockingTombstoneDetails.NumberOfTombstones)] = BlockingTombstones[key]
                    });
                }

                return new DynamicJsonValue()
                {
                    [nameof(BlockingTombstones)] = djv
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

