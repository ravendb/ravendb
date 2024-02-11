using System;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.CompareExchange
{
    public sealed class CompareExchangeValue<T> : ICompareExchangeValue
    {
        /// <inheritdoc cref="ICompareExchangeValue.Key"/>
        public string Key { get; }

        /// <inheritdoc cref="ICompareExchangeValue.Index"/>
        public long Index { get; internal set; }

        /// <inheritdoc cref="ICompareExchangeValue.Value"/>
        public T Value { get; set; }

        public string ChangeVector { get; internal set; }

        /// <inheritdoc cref="ICompareExchangeValue.Metadata"/>
        public IMetadataDictionary Metadata => _metadataAsDictionary ??= new MetadataAsDictionary();

        private IMetadataDictionary _metadataAsDictionary;

        private bool HasMetadata => _metadataAsDictionary != null;

        string ICompareExchangeValue.Key => Key;

        long ICompareExchangeValue.Index { get => Index; set => Index = value; }

        object ICompareExchangeValue.Value => Value;

        IMetadataDictionary ICompareExchangeValue.Metadata => Metadata;
        bool ICompareExchangeValue.HasMetadata => HasMetadata;

        public CompareExchangeValue(string key, long index, T value, IMetadataDictionary metadata = null)
            : this(key, index, value, changeVector: null, metadata)
        {

        }

        internal CompareExchangeValue(string key, long index, T value, string changeVector, IMetadataDictionary metadata)
        {
            Key = key;
            Index = index;
            Value = value;
            ChangeVector = changeVector;
            _metadataAsDictionary = metadata;
        }

        internal static CompareExchangeValue<BlittableJsonReaderObject> CreateFrom(BlittableJsonReaderObject json)
        {
            if (json == null)
                return null;

            if (json.TryGet(nameof(Key), out string key) == false)
                throw new InvalidOperationException("");

            if (json.TryGet(nameof(Index), out long index) == false)
                throw new InvalidOperationException("");

            if (json.TryGet(nameof(Value), out BlittableJsonReaderObject value) == false)
                throw new InvalidOperationException("");

            json.TryGet(nameof(ChangeVector), out string changeVector);

            return new CompareExchangeValue<BlittableJsonReaderObject>(key, index, value, changeVector, metadata: null);
        }
    }

    internal interface ICompareExchangeValue
    {
        /// <summary>
        /// The unique key of the compare request entry
        /// </summary>
        public string Key { get; }

        /// <summary>
        /// Current index of the entry, used for concurrency checks.<br/>
        /// </summary>
        public long Index { get; internal set; }
        
        /// <summary>
        /// The value of the entry
        /// </summary>
        public object Value { get; }

        /// <summary>
        /// Access to the metadata of the compare exchange value
        /// </summary>
        public IMetadataDictionary Metadata { get; }
        public bool HasMetadata { get; }
    }
}
