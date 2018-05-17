
namespace Raven.Client.Exceptions
{
    public class LegacyReplicationViolationException : System.Exception
    {
        public LegacyReplicationViolationException() { }
        public LegacyReplicationViolationException(string message) : base(message) { }
        public LegacyReplicationViolationException(string message, System.Exception inner) : base(message, inner) { }
    }
}
