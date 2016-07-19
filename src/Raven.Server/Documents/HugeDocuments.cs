using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Database.Util;

namespace Raven.Server.Documents
{
    public class HugeDocuments
    {
        private readonly SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, int> _hugeDocs;
        private readonly int _maxWarnSize;

        public HugeDocuments(int maxCollectionSize, int maxWarnSize)
        {
            _maxWarnSize = maxWarnSize;
            _hugeDocs = new SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, int>(maxCollectionSize);
        }

        public void AddIfDocIsHuge(string id, int size)
        {
            if (size > _maxWarnSize)
                _hugeDocs.Set(new Tuple<string, DateTime>(id, DateTime.UtcNow), size);
        }

        public SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, int> GetHugeDocuments()
        {
            return _hugeDocs;
        } 
    }
}