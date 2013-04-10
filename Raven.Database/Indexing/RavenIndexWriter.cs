// -----------------------------------------------------------------------
//  <copyright file="RavenIndexWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Abstractions.Logging;

namespace Raven.Database.Indexing
{
	public class RavenIndexWriter : IDisposable
	{
		private static readonly ILog LogIndexing = LogManager.GetLogger(typeof(Index).FullName + ".Indexing");

		private readonly int maximumNumberOfWritesBeforeRecreate;

		private IndexWriter indexWriter;

		private readonly Directory directory;

		private readonly Analyzer analyzer;

		private readonly IndexDeletionPolicy indexDeletionPolicy;

		private readonly IndexWriter.MaxFieldLength maxFieldLength;

		private int currentNumberOfWrites;

		public Directory Directory
		{
			get
			{
				if (indexWriter != null)
					return indexWriter.Directory;

				return null;
			}
		}

		public Analyzer Analyzer
		{
			get
			{
				if (indexWriter != null)
					return indexWriter.Analyzer;

				return null;
			}
		}

		public RavenIndexWriter(Directory d, Analyzer a, IndexDeletionPolicy deletionPolicy, IndexWriter.MaxFieldLength mfl, int maximumNumberOfWritesBeforeRecreate)
		{
			directory = d;
			analyzer = a;
			indexDeletionPolicy = deletionPolicy;
			maxFieldLength = mfl;

			this.maximumNumberOfWritesBeforeRecreate = maximumNumberOfWritesBeforeRecreate;

			RecreateIfNecessary();
		}

		public void AddDocument(Document doc)
		{
			RecreateIfNecessary();

			indexWriter.AddDocument(doc);
			currentNumberOfWrites++;
		}

		public void AddDocument(Document doc, Analyzer a)
		{
			RecreateIfNecessary();

			indexWriter.AddDocument(doc, a);
			currentNumberOfWrites++;
		}

		public void DeleteDocuments(Term term)
		{
			RecreateIfNecessary();

			indexWriter.DeleteDocuments(term);

			currentNumberOfWrites += 2; // deletes are more expensive than additions
		}

		public void DeleteDocuments(Term[] terms)
		{
			RecreateIfNecessary();

			indexWriter.DeleteDocuments(terms);

			currentNumberOfWrites += terms.Length*2; // deletes are more expensive than writes
		}

		public IndexReader GetReader()
		{
			return indexWriter.GetReader();
		}

		public void Commit()
		{
			indexWriter.Commit();
		}

		public void Optimize()
		{
			indexWriter.Optimize();
		}

		private void RecreateIfNecessary()
		{
			if (currentNumberOfWrites >= maximumNumberOfWritesBeforeRecreate)
				DisposeIndexWriter();

			if (indexWriter == null)
				CreateIndexWriter();
		}

		private void CreateIndexWriter()
		{
			indexWriter = new IndexWriter(directory, analyzer, indexDeletionPolicy, maxFieldLength);
			using (indexWriter.MergeScheduler) { }
			indexWriter.SetMergeScheduler(new ErrorLoggingConcurrentMergeScheduler());

			// RavenDB already manages the memory for those, no need for Lucene to do this as well
			indexWriter.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
			indexWriter.SetRAMBufferSizeMB(1024);

			currentNumberOfWrites = 0;
		}

		private void DisposeIndexWriter()
		{
			if (indexWriter == null)
				return;

			var writer = indexWriter;
			indexWriter = null;

			try
			{
				writer.Analyzer.Close();
			}
			catch (Exception e)
			{
				LogIndexing.ErrorException("Error while closing the index (closing the analyzer failed)", e);
			}

			try
			{
				writer.Dispose();
			}
			catch (Exception e)
			{
				LogIndexing.ErrorException("Error when closing the index", e);
			}
		}

		public void Dispose()
		{
			if (indexWriter != null)
				indexWriter.Dispose();

			indexWriter = null;
		}

		public void Dispose(bool waitForMerges)
		{
			if (indexWriter != null)
				indexWriter.Dispose(waitForMerges);

			indexWriter = null;
		}
	}
}