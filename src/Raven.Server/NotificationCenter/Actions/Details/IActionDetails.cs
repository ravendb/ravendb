using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Details
{
    public interface IActionDetails
    {
        DynamicJsonValue ToJson();
    }
}