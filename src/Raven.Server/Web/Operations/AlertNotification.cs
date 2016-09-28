using Raven.Client.Data;
using Raven.Server.Documents;

namespace Raven.Server.Web.Operations
{
    public class AlertNotification : Notification
    {
        public Alert Alert { get; set; }
        
        public bool Global { get; set; }

    }
}