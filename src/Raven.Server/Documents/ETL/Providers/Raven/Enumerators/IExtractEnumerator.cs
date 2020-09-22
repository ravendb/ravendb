using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public interface IExtractEnumerator<out T> : IEnumerator<T> where T : ExtractedItem
    {
        // we must filter after the extract to maintain the etag order
        public bool Filter();
    }
}
