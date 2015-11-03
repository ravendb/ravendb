using System;

namespace Raven.Database.Server.Connections
{
    public interface IEventsTransport : IDisposable
    {
        string Id { get; }
        string ResourceName { get; set; }
        bool Connected { get; set; }
        long CoolDownWithDataLossInMiliseconds { get; set; }
        TimeSpan Age { get; }

        event Action Disconnected;
        void SendAsync(object msg);
    }
}
