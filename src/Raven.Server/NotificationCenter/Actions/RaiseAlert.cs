using Raven.Server.NotificationCenter.Actions.Details;
using Raven.Server.NotificationCenter.Alerts;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions
{
    public class RaiseAlert : Action
    {
        private RaiseAlert()
        {
        }

        public AlertSeverity Severity { get; set; }

        public AlertType AlertType { get; set; }

        public string Key { get; set; }

        public override string Id => string.IsNullOrEmpty(Key) ? Type.ToString() : $"{Type}/{Key}";

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            
            json[nameof(Key)] = Key;
            json[nameof(Severity)] = Severity.ToString();
            json[nameof(AlertType)] = AlertType.ToString();

            return json;
        }

        public static RaiseAlert Create(string title, string msg, AlertType type, AlertSeverity severity, string key = null, IActionDetails details = null)
        {
            return new RaiseAlert
            {
                IsPersistent = true,
                Title = title,
                Message = msg,
                Type = ActionType.Alert,
                AlertType = type,
                Severity = severity,
                Key = key,
                Details = details
            };
        }
    }
}