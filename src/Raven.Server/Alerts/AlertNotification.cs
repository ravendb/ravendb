using Raven.Client.Data;

namespace Raven.Server.Alerts
{
    public class AlertNotification : Notification
    {
        public Alert Alert { get; set; }
        
        public bool Global { get; set; }

    }
}