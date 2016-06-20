using System;
using System.Collections.Generic;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Store;

using Raven.Abstractions.Logging;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;

using Sparrow.Json;

using Voron.Impl;

using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexWriteOperation : IndexOperationBase
    {
        private readonly ILog _log = LogManager.GetLogger(typeof(IndexWriteOperation));

        private readonly Term _documentId = new Term(Constants.DocumentIdFieldName, "Dummy");
        private readonly Term _reduceKeyHash = new Term(Constants.ReduceKeyFieldName, "Dummy");

        private readonly string _name;

        private readonly LuceneIndexWriter _writer;
        private readonly LuceneDocumentConverterBase _converter;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly Lock _locker;
        private readonly IDisposable _releaseWriteTransaction;

        public IndexWriteOperation(string name, Dictionary<string, IndexField> fields, LuceneVoronDirectory directory, LuceneDocumentConverterBase converter, Transaction writeTransaction, LuceneIndexPersistence persistence)
        {
            _name = name;
            _converter = converter;

            try
            {
                _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), fields);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }

            try
            {
                _releaseWriteTransaction = directory.SetTransaction(writeTransaction);

                _writer = persistence.EnsureIndexWriter();

                _locker = directory.MakeLock("writing-to-index.lock");

                if (_locker.Obtain() == false)
                    throw new InvalidOperationException($"Could not obtain the 'writing-to-index' lock for '{name}' index.");
            }
            catch (Exception e)
            {
                throw new IndexWriteException(e);
            }
        }

        public override void Dispose()
        {
            try
            {
                if (_writer != null) // TODO && _persistance._indexWriter.RamSizeInBytes() >= long.MaxValue)
                    _writer.Commit(); // just make sure changes are flushed to disk

                _releaseWriteTransaction?.Dispose();
            }
            finally
            {
                _locker?.Release();
                _analyzer?.Dispose();
            }
        }

        public void IndexDocument(LazyStringValue key, object document, IndexingStatsScope stats)
        {
            global::Lucene.Net.Documents.Document luceneDoc;
            using (stats.For("Lucene_ConvertTo"))
                luceneDoc = _converter.ConvertToCachedDocument(key, document);

            Delete(key, stats);

            using (stats.For("Lucene_AddDocument"))
                _writer.AddDocument(luceneDoc, _analyzer);

            stats.RecordIndexingOutput(); // TODO [ppekrol] in future we will have to support multiple index outputs from single document

            if (_log.IsDebugEnabled)
                _log.Debug($"Indexed document for '{_name}'. Key: {key}. Output: {luceneDoc}.");
        }

        public void Delete(LazyStringValue key, IndexingStatsScope stats)
        {
            using (stats.For("Lucene_Delete"))
                _writer.DeleteDocuments(_documentId.CreateTerm(key));

            if (_log.IsDebugEnabled)
                _log.Debug($"Deleted document for '{_name}'. Key: {key}.");
        }

        public void DeleteReduceResult(string reduceKeyHash, IndexingStatsScope stats)
        {
            using (stats.For("Lucene_Delete"))
                _writer.DeleteDocuments(_reduceKeyHash.CreateTerm(reduceKeyHash));

            if (_log.IsDebugEnabled)
                _log.Debug($"Deleted document for '{_name}'. Reduce key hash: {reduceKeyHash}.");
        }
    }
}