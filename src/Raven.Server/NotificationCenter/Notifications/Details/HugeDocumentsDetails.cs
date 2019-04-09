using System;
using System.Collections.Concurrent;
using System.Linq;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class HugeDocumentsDetails : INotificationDetails
    {
        private const int SizeLimit = 100;
        private const double CleanupFactor = 0.3;
        
        public ConcurrentDictionary<string, HugeDocumentInfo> HugeDocuments { get; set; }

        public HugeDocumentsDetails()
        {
            HugeDocuments = new ConcurrentDictionary<string, HugeDocumentInfo>();
        }
        
        public void Update(string id, int size)
        {
            var documentInfo = new HugeDocumentInfo(size, id);
            HugeDocuments[id] = documentInfo;
         
            EnforceLimits();
        }

        private void EnforceLimits()
        {
            if (HugeDocuments.Count > SizeLimit)
            {
                // since we don't want to compare all dates every time when our collections becomes full
                // we delete 30% of items in single run
                var oldItems = HugeDocuments.Values
                    .OrderBy(x => x.Date)
                    .Take((int)Math.Floor(SizeLimit * CleanupFactor))
                    .Select(x => x.Id)
                    .ToList();
                
                foreach (var oldItem in oldItems)
                {
                    HugeDocuments.TryRemove(oldItem, out var _);
                }
            }
        }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            
            var dict = new DynamicJsonValue();
            djv[nameof(HugeDocuments)] = dict;
            
            foreach (var key in HugeDocuments.Keys)
            {
                var info = HugeDocuments[key];
                dict[key] = info.ToJson();
            }

            return djv;
        }
    }

    public struct HugeDocumentInfo : IDynamicJsonValueConvertible
    {
        public long Size;
        public DateTime Date;
        public string Id;

        public HugeDocumentInfo(long size, string id)
        {
            Size = size;
            Id = id;
            Date = DateTime.UtcNow;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Size)] = Size,
                [nameof(Date)] = Date,
                [nameof(Id)] = Id
            };
        }
    }
}
