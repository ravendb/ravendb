using Raven.Server.NotificationCenter.Actions.Details;
using Raven.Server.NotificationCenter.Alerts;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Server
{
    public class RaiseServerAlert : ServerAction
    {
        private RaiseServerAlert()
        {
        }

        public AlertSeverity Severity { get; set; }

        public ServerAlertType AlertType { get; set; }

        public string Key { get; set; }

        public override string Id => AlertUtil.CreateId(AlertType, Key);

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            
            json[nameof(Key)] = Key;
            json[nameof(Severity)] = Severity.ToString();
            json[nameof(AlertType)] = AlertType.ToString();

            return json;
        }

        public static RaiseServerAlert Create(string title, string msg, ServerAlertType type, AlertSeverity severity, string key = null, IActionDetails details = null)
        {
            return new RaiseServerAlert
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