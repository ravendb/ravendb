namespace Raven.NewClient.Client.Exceptions.Versioning
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