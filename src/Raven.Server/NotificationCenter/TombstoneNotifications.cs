using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public class TombstoneNotifications // blockingTombstones ??????????
    {
        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;
        private readonly Logger _logger;

        public TombstoneNotifications(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
            _logger = LoggingSource.Instance.GetLogger(database, GetType().FullName);
        }

        public void Add(Dictionary<(string, string), long> blockingTombstones)
        {
            BlockingTombstonesDetails details = new BlockingTombstonesDetails(blockingTombstones);
            var x = details.ToJson(); // to remove
            string msg = $"We have detected blocking of tombstones deletion. Consider deleting or enabling the following processes:";
            _notificationCenter.Add(AlertRaised.Create(_database, msg, msg, AlertType.BlockingTombstones,
                NotificationSeverity.Warning,
                nameof(AlertType.BlockingTombstones), details: details));
        }

        internal class BlockingTombstonesDetails : INotificationDetails
        {
            public BlockingTombstonesDetails(Dictionary<(string, string),  long> blockingTombstones)
            {
                BlockingTombstones = blockingTombstones;
            }

            public Dictionary<(string, string), long> BlockingTombstones { get; set; }

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonArray();
                foreach (var key in BlockingTombstones.Keys)
                {
                    djv.Add( new DynamicJsonValue
                    {
                        [nameof(BlockingTombstoneDetails.Source)] = key.Item1,
                        [nameof(BlockingTombstoneDetails.Collection)] = key.Item2,
                        [nameof(BlockingTombstoneDetails.NumberOfTombstones)] = BlockingTombstones[key]
                    });
                }

                return new DynamicJsonValue()
                {
                    [nameof(BlockingTombstones)] = djv
                };
            }
        }

        internal class BlockingTombstoneDetails
        {
            public string Source { get; }
            public string Collection { get; }
            public long NumberOfTombstones { get; }
        }
    }
}

