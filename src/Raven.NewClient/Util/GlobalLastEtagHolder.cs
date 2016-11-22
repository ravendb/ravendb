using Raven.NewClient.Abstractions.Data;

namespace Raven.NewClient.Client.Util
{
    public class GlobalLastEtagHolder : ILastEtagHolder
    {
        private class EtagHolder
        {
            public long? Etag;
        }

        private volatile EtagHolder lastEtag;
        protected readonly object lastEtagLocker = new object();

        public void UpdateLastWrittenEtag(long? etag)
        {
            if (etag == null)
                return;

            if (lastEtag == null)
            {
                lock (lastEtagLocker)
                {
                    if (lastEtag == null)
                    {
                        lastEtag = new EtagHolder
                        {
                            Etag = etag
                        };
                        return;
                    }
                }
            }

            // not the most recent etag
            if (lastEtag.Etag.Value.CompareTo(etag.Value) >= 0)
            {
                return;
            }

            lock (lastEtagLocker)
            {
                // not the most recent etag
                if (lastEtag.Etag.Value.CompareTo(etag.Value) >= 0)
                {
                    return;
                }

                lastEtag = new EtagHolder
                {
                   Etag = etag,
                };
            }
        }

        
        public long? GetLastWrittenEtag()
        {
            var etagHolder = lastEtag;
            if (etagHolder == null)
                return null;
            return etagHolder.Etag;
        }
    }
}
