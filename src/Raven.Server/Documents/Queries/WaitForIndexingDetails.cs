
namespace Raven.Server.Documents.Queries
{
    public sealed class WaitForIndexingDetails
    {
        public string Collection { get; set; }
        public long Etag { get; set; }
    }
}
