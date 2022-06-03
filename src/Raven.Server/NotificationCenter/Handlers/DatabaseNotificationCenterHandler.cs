using System;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class DatabaseNotificationCenterHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/notification-center/watch", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task Get()
        {
            using (var processor = new DatabaseNotificationCenterHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/notification-center/dismiss", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task DismissPost()
        {
            var id = GetStringQueryString("id");
            var forever = GetBoolValueQueryString("forever", required: false);

            if (forever == true)
                Database.NotificationCenter.Postpone(id, DateTime.MaxValue);
            else
                Database.NotificationCenter.Dismiss(id);

            return NoContent();
        }

        [RavenAction("/databases/*/notification-center/postpone", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task PostponePost()
        {
            var id = GetStringQueryString("id");
            var timeInSec = GetLongQueryString("timeInSec");

            var until = timeInSec == 0 ? DateTime.MaxValue : SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec));
            Database.NotificationCenter.Postpone(id, until);

            return NoContent();
        }
    }
}
