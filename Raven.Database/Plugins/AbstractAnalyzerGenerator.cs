using System.ComponentModel.Composition;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractAnalyzerGenerator
	{
		public abstract Analyzer GenerateAnalyzerForIndexing(string indexName, Document document, Analyzer previousAnalyzer);

		public abstract Analyzer GenerateAnalzyerForQuerying(string indexName, string query, Analyzer previousAnalyzer);
	}
}