using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public interface IInitialMessage
    {
    }

    public sealed class StartupMessage : IInitialMessage
    {
        public ProtocolVersion ProtocolVersion;
        public Dictionary<string, string> ClientOptions;
    }

    public sealed class Cancel : IInitialMessage
    {
        public int ProcessId;
        public int SessionId;
    }

    public sealed class SSLRequest : IInitialMessage
    {
    }
}
