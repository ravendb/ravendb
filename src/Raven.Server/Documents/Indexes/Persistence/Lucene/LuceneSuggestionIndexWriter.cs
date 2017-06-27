using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public sealed class LuceneSuggestionIndexWriter : IDisposable
    {
        private const string F_WORD = "word";
        private readonly Term F_WORD_TERM = new Term(F_WORD);

        private readonly string _field;
        private readonly LuceneVoronDirectory _directory;
        private LowerCaseWhitespaceAnalyzer _analyzer = new LowerCaseWhitespaceAnalyzer();
        private readonly SnapshotDeletionPolicy _indexDeletionPolicy;
        private readonly IndexWriter.MaxFieldLength _maxFieldLength;

        private IndexWriter _indexWriter;
        private IndexSearcher _indexSearcher;

        private readonly Logger _logger;

        public Analyzer Analyzer => _indexWriter?.Analyzer;

        public LuceneSuggestionIndexWriter(string field, LuceneVoronDirectory directory, SnapshotDeletionPolicy snapshotter, IndexWriter.MaxFieldLength maxFieldLength, DocumentDatabase database, IState state)
        {
            this._directory = directory;
            this._indexDeletionPolicy = snapshotter;
            this._maxFieldLength = maxFieldLength; 
            this._field = field;

            _logger = LoggingSource.Instance.GetLogger<LuceneSuggestionIndexWriter>(database.Name);
            
            RecreateIndexWriter(state);                        
        }

        private int GetMin(int l)
        {
            if (l > 5)
            {
                return 3;
            }
            if (l == 5)
            {
                return 2;
            }
            return 1;
        }


        private int GetMax(int l)
        {
            if (l > 5)
            {
                return 4;
            }
            if (l == 5)
            {
                return 3;
            }
            return 2;
        }

        private static void AddGram(string text, global::Lucene.Net.Documents.Document doc, int ng1, int ng2)
        {
            int len = text.Length;
            for (int ng = ng1; ng <= ng2; ng++)
            {
                string key = "gram" + ng;
                string end = null;
                for (int i = 0; i < len - ng + 1; i++)
                {
                    string gram = text.Substring(i, (i + ng) - (i));
                    doc.Add(new Field(key, gram, Field.Store.NO, Field.Index.NOT_ANALYZED));
                    if (i == 0)
                    {
                        doc.Add(new Field("start" + ng, gram, Field.Store.NO, Field.Index.NOT_ANALYZED));
                    }
                    end = gram;
                }
                if (end != null)
                {
                    // may not be present if len==ng1
                    doc.Add(new Field("end" + ng, end, Field.Store.NO, Field.Index.NOT_ANALYZED));
                }
            }
        }

        private static global::Lucene.Net.Documents.Document CreateDocument(string text, int ng1, int ng2)
        {
            global::Lucene.Net.Documents.Document doc = new global::Lucene.Net.Documents.Document();
            doc.Add(new Field(F_WORD, text, Field.Store.YES, Field.Index.NOT_ANALYZED)); // orig term
            AddGram(text, doc, ng1, ng2);
            return doc;
        }

        public void AddDocument(global::Lucene.Net.Documents.Document doc, IState state)
        {           
            var fieldables = doc.GetFieldables(_field);
            if (fieldables == null)
                return;

            foreach (var fieldable in fieldables)
            {
                if (fieldable == null)
                    continue;

                TextReader reader;
                var str = fieldable.StringValue(state);
                if (!string.IsNullOrEmpty(str))
                {
                    reader = new StringReader(str);
                }
                else
                {
                    // We are reusing the fieldable for indexing. Instead of recreating it, we just reset the underlying text reader.
                    reader = fieldable.ReaderValue;
                    if (reader is ReusableStringReader stringReader)
                    {
                        if (stringReader.Length == 0)
                            continue;

                        stringReader.Reset();
                    }
                    else if (reader is StreamReader streamReader)
                    {
                        if (streamReader.BaseStream.Length == 0)
                            continue;

                        streamReader.BaseStream.Position = 0;
                    }
                    else continue;
                }                
                
                var tokenStream = _analyzer.ReusableTokenStream(_field, reader);
                while (tokenStream.IncrementToken())
                {
                    var word = tokenStream.GetAttribute<ITermAttribute>().Term;

                    // Index
                    int len = word.Length;
                    if (len < 3)
                    {
                        continue; // too short we bail but "too long" is fine...
                    }

                    if (_indexSearcher.DocFreq(F_WORD_TERM.CreateTerm(word), state) > 0)
                    {
                        // if the word already exist in the gramindex
                        continue;
                    }

                    _indexWriter.AddDocument(CreateDocument(word, GetMin(len), GetMax(len)), state);
                }
            }
        }

        public void DeleteDocuments(Term term, IState state)
        {
        }

        public void Commit(IState state)
        {
            try
            {
                _indexWriter.Commit(state);
            }
            catch (SystemException e)
            {
                if (e.Message.StartsWith("this writer hit an OutOfMemoryError"))
                {
                    RecreateIndexWriter(state);
                    throw new OutOfMemoryException("Index writer hit OOM during commit", e);
                }

                throw;
            }

            RecreateIndexWriter(state);
        }

        public long RamSizeInBytes()
        {
            return _indexWriter.RamSizeInBytes();
        }

        private void RecreateIndexWriter(IState state)
        {
            try
            {
                DisposeIndexWriter();

                if (_indexWriter == null)
                    CreateIndexWriter(state);
            }
            catch (Exception e)
            {
                throw new IndexWriterCreationException(e);
            }
        }

        private void CreateIndexWriter(IState state)
        {
            _indexWriter = new IndexWriter(_directory, _analyzer, _indexDeletionPolicy, _maxFieldLength, state)
            {                
                UseCompoundFile = false
            };           

            using (_indexWriter.MergeScheduler)
            {
            }
            
            _indexWriter.SetMergeScheduler(new SerialMergeScheduler(), state);

            // RavenDB already manages the memory for those, no need for Lucene to do this as well
            _indexWriter.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
            _indexWriter.SetRAMBufferSizeMB(50);
            _indexWriter.MergeFactor = 300;

            _indexSearcher = new IndexSearcher(_directory, true, state);

            _analyzer = new LowerCaseWhitespaceAnalyzer();
        }

        private void DisposeIndexWriter(bool waitForMerges = true)
        {
            if (_indexWriter == null)
                return;

            var searcher = _indexSearcher;
            _indexSearcher = null;
            
            var writer = _indexWriter;
            _indexWriter = null;

            try
            {
                writer?.Analyzer.Close();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Error while closing the suggestions index for field '{_field}' (closing the analyzer failed)", e);
            }

            try
            {
                writer.Dispose(waitForMerges);
                searcher?.Dispose();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Error when closing the suggestions index for field '{_field}'", e);
            }
        }

        public void Dispose()
        {
            DisposeIndexWriter();
        }
    }
}
