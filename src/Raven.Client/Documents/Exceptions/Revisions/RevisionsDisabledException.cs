using Raven.Client.Exceptions;

namespace Raven.Client.Documents.Exceptions.Revisions
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