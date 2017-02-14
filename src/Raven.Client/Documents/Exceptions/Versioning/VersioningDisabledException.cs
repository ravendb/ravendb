using Raven.Client.Exceptions;

namespace Raven.Client.Documents.Exceptions.Versioning
{
    public class VersioningDisabledException : RavenException
    {
        public VersioningDisabledException() : base("Versioning is disabled")
        {
        }

        public VersioningDisabledException(string message) : base(message)
        {
        }
    }
}