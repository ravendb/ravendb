using Raven.Abstractions.Data;

namespace Raven.NewClient.Client.Util
{
    public interface ILastEtagHolder
    {
        void UpdateLastWrittenEtag(long? etag);
        long? GetLastWrittenEtag();
    }
}
