using System;

using Raven.Abstractions.Logging;

namespace Raven.Database.Server.Connections
{
    public interface ILogsTransport : IDisposable
    {
        string Id { get; }
        bool Connected { get; set; }

        event Action Disconnected;
        void SendAsync(LogEventInfo msg);
    }
}
