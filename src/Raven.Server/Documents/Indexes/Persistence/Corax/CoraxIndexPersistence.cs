using System;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Indexing;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Impl;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxIndexPersistence : IndexPersistenceBase
{
    private readonly Logger _logger;
    private readonly CoraxDocumentConverterBase _converter;

    public CoraxIndexPersistence(Index index) : base(index)
    {
        _logger = LoggingSource.Instance.GetLogger<CoraxIndexPersistence>(index.DocumentDatabase.Name);
        bool storeValue = false;
        switch (index.Type)
        {
            case IndexType.AutoMapReduce:
                storeValue = true;
                break;
            case IndexType.MapReduce:
                _converter = new AnonymousCoraxDocumentConverter(index, true);
                break;
            case IndexType.Map:
                _converter = new AnonymousCoraxDocumentConverter(index);
                break;
            case IndexType.JavaScriptMap:
                switch (_index.SourceType)
                {
                    case IndexSourceType.Documents:
                        _converter = new JintCoraxDocumentConverter((MapIndex)index);
                        break;
                    case IndexSourceType.TimeSeries:
                        throw new NotSupportedException($"Currently, {nameof(TimeSeries)} are not supported by Corax");
                    case IndexSourceType.Counters:
                        throw new NotSupportedException($"Currently, {nameof(IndexSourceType.Counters)} are not supported by Corax");
                }
                break;
            case IndexType.JavaScriptMapReduce:
                _converter = new JintCoraxDocumentConverter((MapReduceIndex)index, storeValue: true);
                break;
        }
        _converter ??= new CoraxDocumentConverter(index, storeValue: storeValue);
    }
    
    public override IndexReadOperationBase OpenIndexReader(Transaction readTransaction) => new CoraxIndexReadOperation(_index, _logger, readTransaction);

    public override bool ContainsField(string field)
    {
        if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
            return _index.Type.IsMap();

        return _index.Definition.IndexFields.ContainsKey(field);
    }

    public override IndexFacetedReadOperation OpenFacetedIndexReader(Transaction readTransaction)
    {
        throw new NotImplementedException($"Facet is not supported by {nameof(Corax)} yet.");
    }

    public override SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field)
    {
        if (_converter.GetKnownFields().TryGetByFieldName(field, out var binding) == false)
            throw new InvalidOperationException($"No suggestions index found for field '{field}'.");

        return new CoraxSuggestionReader(_index, _logger, binding, readTransaction);
    }

    public override void Dispose()
    {
        _converter?.Dispose();
    }

    #region LuceneMethods

    public override bool HasWriter { get; }

    public override void CleanWritersIfNeeded()
    {
        // lucene method
    }

    public override void Clean(IndexCleanup mode)
    {
        // lucene method
    }

    public override void Initialize(StorageEnvironment environment)
    {
        // lucene method
    }

    public override void PublishIndexCacheToNewTransactions(IndexTransactionCache transactionCache)
    {
        //lucene method
    }

    internal override IndexTransactionCache BuildStreamCacheAfterTx(Transaction tx)
    {
        //lucene method

        return null;
    }

    internal override void RecreateSearcher(Transaction asOfTx)
    {
        //lucene method
    }

    internal override void RecreateSuggestionsSearchers(Transaction asOfTx)
    {
        //lucene method
    }

    public override void DisposeWriters()
    {
        //lucene method
    }
    #endregion
    

    public override IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext)
    {
        return new CoraxIndexWriteOperation(
            _index,
            writeTransaction,
            _converter,
            _logger
        );
    }
    
}
