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
using Voron.Impl;

using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexWriteOperation : IndexOperationBase
    {
        private readonly ILog Log = LogManager.GetLogger(typeof(IndexWriteOperation));

        private readonly Term _documentId = new Term(Constants.DocumentIdFieldName, "Dummy");
        private readonly Term _reduceKeyHash = new Term(Constants.ReduceKeyFieldName, "Dummy");

        private readonly string _name;

        private readonly LuceneIndexWriter _writer;
        private readonly LuceneDocumentConverter _converter;
        private readonly LuceneIndexPersistence _persistence;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly Lock _locker;
        private readonly IDisposable _releaseWriteTransaction;

        public IndexWriteOperation(string name, Dictionary<string, IndexField> fields, LuceneVoronDirectory directory, LuceneDocumentConverter converter, Transaction writeTransaction, LuceneIndexPersistence persistence)
        {
            _name = name;
            _converter = converter;
            _persistence = persistence;

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

                _writer = _persistence.EnsureIndexWriter();

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

        public void IndexDocument(Document document)
        {
            var luceneDoc = _converter.ConvertToCachedDocument(document);

            _writer.AddDocument(luceneDoc, _analyzer);

            if (Log.IsDebugEnabled)
                Log.Debug($"Indexed document for '{_name}'. Key: {document.Key} Etag: {document.Etag}. Output: {luceneDoc}.");
        }

        public void Delete(string key)
        {
            _writer.DeleteDocuments(_documentId.CreateTerm(key));

            if (Log.IsDebugEnabled)
                Log.Debug($"Deleted document for '{_name}'. Key: {key}.");
        }

        public void DeleteReduceResult(ulong reduceKeyHash)
        {
            _writer.DeleteDocuments(_reduceKeyHash.CreateTerm(reduceKeyHash.ToString())); // TODO arek - ToString call

            if (Log.IsDebugEnabled)
                Log.Debug($"Deleted document for '{_name}'. Reduce key hash: {reduceKeyHash}.");
        }
    }
}