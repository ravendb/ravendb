namespace Raven.Server.Integrations.PostgreSQL
{
    public enum ProtocolVersion
    {
        Version3 = 0x00030000,
        TlsConnection = 80877103,
        CancelMessage = 080877102
    }
}
