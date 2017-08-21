namespace Raven.Client.Exceptions.Documents.Revisions
{
    public class RevisionsDisabledException : RavenException
    {
        public RevisionsDisabledException() : base("Revisions are disabled")
        {
        }

        public RevisionsDisabledException(string message) : base(message)
        {
        }
    }
}