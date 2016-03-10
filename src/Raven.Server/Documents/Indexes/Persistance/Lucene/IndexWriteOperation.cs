using System;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes.Persistance.Lucene.Documents;
using Raven.Server.Indexing;
using Voron.Impl;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene
{
    public class IndexWriteOperation : IDisposable
    {
        private readonly Term _documentId = new Term(Constants.DocumentIdFieldName, "Dummy");
        private readonly object _writeLock;
        private readonly LuceneIndexWriter _writer;
        private readonly LuceneDocumentConverter _converter;
        private readonly LuceneIndexPersistence _persistence;
        private readonly LowerCaseKeywordAnalyzer _analyzer;
        private readonly Lock _locker;
        private readonly IDisposable _releaseWriteTransaction;

        public IndexWriteOperation(object writeLock, LuceneVoronDirectory directory, LuceneIndexWriter writer, LuceneDocumentConverter converter, 
                                    Transaction writeTransaction, LuceneIndexPersistence persistence)
        {
            _writeLock = writeLock;
            _writer = writer;
            _converter = converter;
            _persistence = persistence;
            _analyzer = new LowerCaseKeywordAnalyzer();

            Monitor.Enter(_writeLock);

            try
            {
                _releaseWriteTransaction = directory.SetTransaction(writeTransaction);

                _persistence.EnsureIndexWriter();
            
                _locker = directory.MakeLock("writing-to-index.lock");

                if (_locker.Obtain() == false)
                    throw new InvalidOperationException();
            }
            catch (Exception)
            {
                Monitor.Exit(_writeLock);
            }
        }

        public void Dispose()
        {
            try
            {
                if (_writer != null) // TODO && _persistance._indexWriter.RamSizeInBytes() >= long.MaxValue)
                    _writer.Commit(); // just make sure changes are flushed to disk

                _persistence.RecreateSearcher();

                _releaseWriteTransaction?.Dispose();
            }
            finally
            {
                _locker?.Release();
                _analyzer?.Dispose();

                Monitor.Exit(_writeLock);
            }
        }

        public void IndexDocument(Document document)
        {
            var luceneDoc = _converter.ConvertToCachedDocument(document);

            _writer.AddDocument(luceneDoc, _analyzer);
        }

        public void Delete(string key)
        {
            _writer.DeleteDocuments(_documentId.CreateTerm(key));
        }
    }
}