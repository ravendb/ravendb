using Sparrow.Json.Parsing;

namespace Raven.Server.Alerts
{
    public interface IAlertContent
    {
        DynamicJsonValue ToJson();
    }
}