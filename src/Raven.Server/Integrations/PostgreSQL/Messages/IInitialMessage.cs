using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    public interface IInitialMessage
    {
    }

    public class StartupMessage : IInitialMessage
    {
        public ProtocolVersion ProtocolVersion;
        public Dictionary<string, string> ClientOptions;
    }

    public class Cancel : IInitialMessage
    {
        public int ProcessId;
        public int SessionId;
    }

    public class SSLRequest : IInitialMessage
    {
    }
}
