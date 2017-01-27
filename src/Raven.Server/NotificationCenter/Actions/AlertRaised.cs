using Raven.Server.NotificationCenter.Actions.Details;
using Raven.Server.NotificationCenter.Alerts;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions
{
    public class AlertRaised : Action
    {
        private AlertRaised() : base(ActionType.AlertRaised)
        {
        }

        public AlertSeverity Severity { get; private set; }

        public AlertType AlertType { get; private set; }

        public string Key { get; private set; }

        public override string Id => string.IsNullOrEmpty(Key) ? $"{Type}/{AlertType}" : $"{Type}/{AlertType}/{Key}";
        
        public IActionDetails Details { get; protected set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            
            json[nameof(Key)] = Key;
            json[nameof(Severity)] = Severity.ToString();
            json[nameof(AlertType)] = AlertType.ToString();
            json[nameof(Details)] = Details?.ToJson();

            return json;
        }

        public static AlertRaised Create(string title, string msg, AlertType type, AlertSeverity severity, string key = null, IActionDetails details = null)
        {
            return new AlertRaised
            {
                IsPersistent = true,
                Title = title,
                Message = msg,
                AlertType = type,
                Severity = severity,
                Key = key,
                Details = details
            };
        }
    }
}