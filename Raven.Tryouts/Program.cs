using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Raven.Client.Document;
using Raven.Client.Embedded;

namespace etobi.EmbeddedTest
{
	internal class Program
	{
		static void Main(string[] args)
		{
			var ramDirectory = FSDirectory.Open(new DirectoryInfo("index"));
			var indexWriter = new IndexWriter(ramDirectory, new StandardAnalyzer());
			var document = new Document();
			document.Add(new Field("id", "1", Field.Store.YES, Field.Index.ANALYZED));
			var field = new Field("First", "Oren", Field.Store.YES, Field.Index.NOT_ANALYZED);
			field.SetBoost(2);
			document.Add(field);

			field = new Field("First", "Eini", Field.Store.YES, Field.Index.NOT_ANALYZED);
			document.Add(field);

			indexWriter.AddDocument(document);

			document = new Document();
			document.Add(new Field("id", "2", Field.Store.YES, Field.Index.ANALYZED));
			field = new Field("First", "Oren", Field.Store.YES, Field.Index.NOT_ANALYZED);
			document.Add(field);

			field = new Field("First", "Eini", Field.Store.YES, Field.Index.NOT_ANALYZED);
			field.SetBoost(2);
			document.Add(field);

			indexWriter.AddDocument(document);
			indexWriter.Flush();
			indexWriter.Close();

			var indexSearcher = new IndexSearcher(ramDirectory);
			var query = new QueryParser("", new WhitespaceAnalyzer()).Parse("Last:Eini");
			var search = indexSearcher.Search(query);
			for (int i = 0; i < search.Length(); i++)
			{
				var explanation = indexSearcher.Explain(query, i);
				var doc = search.Doc(i);
				Console.WriteLine("{0} {1} {2}", doc.GetField("id").StringValue(), search.Score(i), explanation.ToString());
			}
		}
	}
	public class User
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public Guid WindowsAccountId { get; set; }
		public string Email { get; set; }
	}
}