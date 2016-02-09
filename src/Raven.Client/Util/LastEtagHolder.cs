using Raven.Abstractions.Data;

namespace Raven.Client.Util
{
    public interface ILastEtagHolder
    {
        void UpdateLastWrittenEtag(long? etag);
        long? GetLastWrittenEtag();
    }
}
