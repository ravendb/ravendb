using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;

namespace Raven.Client.Document
{
    public abstract class HiLoKeyGeneratorBase
    {
        protected const string RavenKeyGeneratorsHilo = "Raven/Hilo/";
        protected const string RavenKeyServerPrefix = "Raven/ServerPrefixForHilo";

        protected readonly string tag;
        protected long capacity;
        protected long baseCapacity;
        private volatile RangeValue range;

        protected string lastServerPrefix;
        protected DateTime lastRequestedUtc1, lastRequestedUtc2;

        protected readonly ManualResetEvent mre = new ManualResetEvent(false);
        protected InterlockedLock interlockedLock = new InterlockedLock();
        protected long threadsWaitingForRangeUpdate = 0;

        protected HiLoKeyGeneratorBase(string tag, long capacity)
        {
            this.tag = tag;
            this.capacity = capacity;
            baseCapacity = capacity;
            this.range = new RangeValue(1, 0);
        }

        protected string GetDocumentKeyFromId(DocumentConvention convention, long nextId)
        {
            return string.Format("{0}{1}{2}{3}",
                                 tag,
                                 convention.IdentityPartsSeparator,
                                 lastServerPrefix,
                                 nextId);
        }

        protected long GetMaxFromDocument(JsonDocument document, long minMax, long calculatedCapacity)
        {
            long max;
            if (document.DataAsJson.ContainsKey("ServerHi")) // convert from hi to max
            {
                var hi = document.DataAsJson.Value<long>("ServerHi");
                max = ((hi - 1) * calculatedCapacity);
                document.DataAsJson.Remove("ServerHi");
                document.DataAsJson["Max"] = max;
            }
            max = document.DataAsJson.Value<long>("Max");
            return Math.Max(max, minMax);
        }

        protected string HiLoDocumentKey
        {
            get { return RavenKeyGeneratorsHilo + tag; }
        }

        public bool DisableCapacityChanges { get; set; }

        protected void ModifyCapacityIfRequired(ref long calculatedCapacity)
        {
            if (DisableCapacityChanges)
                return;

            var capacityChanged = false;
            var span = SystemTime.UtcNow - lastRequestedUtc1;
            if (span.TotalSeconds < 5)
            {
                capacityChanged = true;
                span = SystemTime.UtcNow - lastRequestedUtc2;
                if (span.TotalSeconds < 3)
                    calculatedCapacity = Math.Max(calculatedCapacity, Math.Max(calculatedCapacity * 2, calculatedCapacity * 4));
                else
                    calculatedCapacity = Math.Max(calculatedCapacity, calculatedCapacity * 2);
            }
            else if (span.TotalMinutes > 1)
            {
                capacityChanged = true;
                calculatedCapacity = Math.Max(baseCapacity, calculatedCapacity / 2);
            }

            if (capacityChanged)
            {
                //there is only one thread that is updating the capacity
                capacity = calculatedCapacity;
            }
                
            lastRequestedUtc2 = lastRequestedUtc1;
            lastRequestedUtc1 = SystemTime.UtcNow;
        }

        protected void IncreaseCapacityIfRequired(ref long calculatedCapacity)
        {
            var waitingForRangeUpdate = Interlocked.Read(ref threadsWaitingForRangeUpdate);
            var capacityChanged = false;

            //we want to have at least 90% of free capacity in the new range
            while ((calculatedCapacity - waitingForRangeUpdate) < 0.9 * calculatedCapacity)
            {
                capacityChanged = true;
                calculatedCapacity = calculatedCapacity * 2;
            }

            if (capacityChanged)
            {
                //there is only one thread that is updating the capacity
                capacity = calculatedCapacity;
            }
        }

        protected JsonDocument HandleGetDocumentResult(MultiLoadResult documents)
        {
            if (documents.Results.Count == 2 && documents.Results[1] != null)
            {
                lastServerPrefix = documents.Results[1].Value<string>("ServerPrefix");
            }
            else
            {
                lastServerPrefix = string.Empty;
            }
            if (documents.Results.Count == 0 || documents.Results[0] == null)
                return null;
            var jsonDocument = documents.Results[0].ToJsonDocument();
            foreach (var key in jsonDocument.Metadata.Keys.Where(x => x.StartsWith("@")).ToArray())
            {
                jsonDocument.Metadata.Remove(key);
            }
            return jsonDocument;
        }

        protected RangeValue Range
        {
            get { return range; }
            set { range = value; }
        }

        [System.Diagnostics.DebuggerDisplay("[{Min}-{Max}]: {Current}")]
        protected class RangeValue
        {
            public readonly long Min;
            public readonly long Max;
            public long Current;

            public RangeValue(long min, long max)
            {
                this.Min = min;
                this.Max = max;
                this.Current = min - 1;
            }
        }
    }
}
