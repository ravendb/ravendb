using System;
using System.Collections.Generic;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;

namespace Raven.Database.Indexing
{
	public interface IIndexExtension : IDisposable
	{
		void OnDocumentsIndexed(IEnumerable<Document> documents, Analyzer searchAnalyzer);
	}
}