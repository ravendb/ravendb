
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Voron;

namespace Raven.Server.Storage.Schema.Updates.Configuration
{
    public class From10 : ISchemaUpdate
    {
        public bool Update(UpdateStep step)
        {
            var table = step.WriteTx.OpenTable(step.ConfigurationStorage.NotificationsStorage._actionsSchema, NotificationsStorage.NotificationsSchema.NotificationsTree);
            
            using (Slice.From(step.WriteTx.Allocator, PerformanceHint.GetKey(PerformanceHintType.SlowIO, string.Empty), out Slice slowIoHintPrefix))
            {
                table.DeleteByPrimaryKeyPrefix(slowIoHintPrefix);
            }

            return true;
        }
    }
}
