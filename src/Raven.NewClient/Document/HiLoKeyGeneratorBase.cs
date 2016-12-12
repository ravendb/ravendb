using System;

namespace Raven.NewClient.Client.Document
{
    public abstract class HiLoKeyGeneratorBase
    {
        protected DocumentStore _store;
        protected readonly string _tag;           
        protected string _prefix;
        protected long _lastBatchSize;
        protected DateTime _lastRangeDate;
        protected readonly string _dbName;
        protected readonly string _identityPartsSeparator;
        private volatile RangeValue _range;

        protected HiLoKeyGeneratorBase(string tag, DocumentStore store, string dbName, string separator)
        {
            _store = store;
            _tag = tag;            
            _dbName = dbName;
            _identityPartsSeparator = separator;
            _range = new RangeValue(1, 0);            
        }

        protected string GetDocumentKeyFromId(long nextId)
        {            
            return string.Format("{0}{1}", _prefix, nextId);
        }

        protected RangeValue Range
        {
            get { return _range; }
            set { _range = value; }
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
