using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Server
{
    public class ResourceChanged : ServerAction
    {
        private ResourceChanged()
        {
            
        }

        public string Name { get; private set; }

        public ResourceChangeType ChangeType { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Message)] = Message,
                [nameof(Type)] = Type.ToString(),
                [nameof(CreatedAt)] = CreatedAt,
                [nameof(Name)] = Name,
                [nameof(ChangeType)] = ChangeType.ToString()
                //[nameof(Details)] = Details?.ToJson()
            };
        }

        public static ResourceChanged Create(string resourceName, ResourceChangeType change)
        {
            return new ResourceChanged
            {
                Type = ActionType.Resource,
                Name = resourceName,
                ChangeType = change
            };
        }
    }
}