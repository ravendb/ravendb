using System.Collections.Generic;
using System.Net.WebSockets;

namespace Raven.Server.Documents
{
    public class NotificationsClientConnectionComparer : IEqualityComparer<NotificationsClientConnection>
    {
        public bool Equals(NotificationsClientConnection x, NotificationsClientConnection y)
        {
            return x == y;
        }

        public int GetHashCode(NotificationsClientConnection obj)
        {
            return obj.GetHashCode();
        }
    }
}