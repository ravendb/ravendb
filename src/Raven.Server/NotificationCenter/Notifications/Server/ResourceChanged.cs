using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Server
{
    public class ResourceChanged : Notification
    {
        private ResourceChanged() : base(NotificationType.ResourceChanged)
        {
        }

        public override string Id => $"{Type}/{ChangeType}/{ResourceName}";

        public string ResourceName { get; private set; }

        public ResourceChangeType ChangeType { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(ResourceName)] = ResourceName;
            json[nameof(ChangeType)] = ChangeType;

            return json;
        }

        public static ResourceChanged Create(string resourceName, ResourceChangeType change)
        {
            return new ResourceChanged
            {
                ResourceName = resourceName,
                ChangeType = change
            };
        }
    }
}