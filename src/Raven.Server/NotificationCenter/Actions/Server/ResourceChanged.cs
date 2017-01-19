using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Server
{
    public class ResourceChanged : ServerAction
    {
        private ResourceChanged()
        {
        }

        public string ResourceName { get; private set; }

        public ResourceChangeType ChangeType { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(ResourceName)] = ResourceName;
            json[nameof(ChangeType)] = ChangeType.ToString();

            return json;
        }

        public static ResourceChanged Create(string resourceName, ResourceChangeType change)
        {
            return new ResourceChanged
            {
                Type = ActionType.Resource,
                ResourceName = resourceName,
                ChangeType = change
            };
        }
    }
}