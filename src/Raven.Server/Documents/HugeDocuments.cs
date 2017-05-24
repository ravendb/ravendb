using System;
using Raven.Client.Util;

namespace Raven.Server.Documents
{
    public class HugeDocuments
    {
        private readonly SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, long> _hugeDocs;
        private readonly long _maxWarnSize;

        public HugeDocuments(int maxCollectionSize, long maxWarnSize)
        {
            _maxWarnSize = maxWarnSize;
            _hugeDocs = new SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, long>(maxCollectionSize);
        }

        public void AddIfDocIsHuge(Document doc)
        {
            if(doc.Data.Size > _maxWarnSize)
                _hugeDocs.Set(new Tuple<string, DateTime>(doc.Id, DateTime.UtcNow), doc.Data.Size);

        }

        public void AddIfDocIsHuge(string id, int size)
        {
            if (size > _maxWarnSize)
                _hugeDocs.Set(new Tuple<string, DateTime>(id, DateTime.UtcNow), size);
        }

        public SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, long> GetHugeDocuments()
        {
            return _hugeDocs;
        } 
    }
}