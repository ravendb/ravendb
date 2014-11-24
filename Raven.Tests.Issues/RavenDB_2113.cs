// -----------------------------------------------------------------------
//  <copyright file="IndexWithEtCollationAnalyzer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Text;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Indexing.Collation.Cultures;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_2113 : RavenTest
	{
		public class Contact
		{
			public string Id { get; set; }

			public string FirstName { get; set; }
		}

		public EmbeddableDocumentStore InitStore()
		{
			var store = NewDocumentStore();
			store.Conventions.MaxNumberOfRequestsPerSession = int.MaxValue;

			store.DatabaseCommands.PutIndex(EtCollationIndex.StaticIndexName, new EtCollationIndex().CreateIndexDefinition());
			store.DatabaseCommands.PutIndex(DeCollationIndex.StaticIndexName, new DeCollationIndex().CreateIndexDefinition());
			store.DatabaseCommands.PutIndex(WithoutCollationIndex.StaticIndexName, new WithoutCollationIndex().CreateIndexDefinition());

			return store;
		}

		[Theory,
		 InlineData(
			 "testing estonian collation sort [???????? ????] ?|testing estonian collation sort [???????? ????] ?testing estonian collation sort [???????? ????] ?testing estonian collation sort [???????? ????] ?testing estonian collation sort [???????? ????] ?testing estonian collation sort [???????? ????] ?testing estonian collation sort [???????? ????] ?testing estonian collation sort [???????? ????] ?testing estonian collation sort [???????? ????] ?testing estonian collation sort [???????? ????] ?"
			 )]
		public void IndexWithEtCollationAnalyzer_CanIndexDocuments_WithLongFieldContent(string sourceData)
		{
			//ARRANGE
			var store = InitStore();
			var session = store.OpenSession();

			var contact1 = new Contact()
			{
				FirstName = sourceData
			};

			session.Store(contact1);

			session.SaveChanges();

			//ACT
			var results = session.Advanced.DocumentQuery<Contact>(EtCollationIndex.StaticIndexName).OrderBy(x => x.FirstName)
				.WaitForNonStaleResults()
				.ToList();

			var stats = store.DatabaseCommands.GetStatistics();

			//ASSERT


			Console.WriteLine("Errors:\n " + string.Join("\n", stats.Errors.ToList()));

			Assert.True(stats.Errors.All(x => x.IndexName != EtCollationIndex.StaticIndexName));

			/*
	{Index: EtCollationIndex, Error: System.ArgumentException: Insufficient array length
   at Raven.Database.Indexing.Collation.IndexableBinaryStringTools_UsingArrays.Encode(Byte[] input, Char[] output) in e:\dev\ravensrc\ravendb-build-2851\Raven.Database\Indexing\Collation\IndexableBinaryStringTools_UsingArrays.cs:line 128
   at Raven.Database.Indexing.Collation.CollationKeyFilter.IncrementToken() in e:\dev\ravensrc\ravendb-build-2851\Raven.Database\Indexing\Collation\CollationKeyFilter.cs:line 37
   at Lucene.Net.Index.DocInverterPerField.ProcessFields(IFieldable[] fields, Int32 count) in c:\Work\lucene.net\src\core\Index\DocInverterPerField.cs:line 149
   at Lucene.Net.Index.DocFieldProcessorPerThread.ProcessDocument() in c:\Work\lucene.net\src\core\Index\DocFieldProcessorPerThread.cs:line 275
   at Lucene.Net.Index.DocumentsWriter.UpdateDocument(Document doc, Analyzer analyzer, Term delTerm) in c:\Work\lucene.net\src\core\Index\DocumentsWriter.cs:line 1031
   at Lucene.Net.Index.DocumentsWriter.AddDocument(Document doc, Analyzer analyzer) in c:\Work\lucene.net\src\core\Index\DocumentsWriter.cs:line 1003
   at Lucene.Net.Index.IndexWriter.AddDocument(Document doc, Analyzer analyzer) in c:\Work\lucene.net\src\core\Index\IndexWriter.cs:line 2331
   at Raven.Database.Indexing.RavenIndexWriter.AddDocument(Document doc, Analyzer a) in e:\dev\ravensrc\ravendb-build-2851\Raven.Database\Indexing\RavenIndexWriter.cs:line 77
   at Raven.Database.Indexing.Index.AddDocumentToIndex(RavenIndexWriter currentIndexWriter, Document luceneDoc, Analyzer analyzer) in e:\dev\ravensrc\ravendb-build-2851\Raven.Database\Indexing\Index.cs:line 688
   at Raven.Database.Indexing.SimpleIndex.<>c__DisplayClass1a.<>c__DisplayClass23.<IndexDocuments>b__12(IEnumerator`1 partition) in e:\dev\ravensrc\ravendb-build-2851\Raven.Database\Indexing\SimpleIndex.cs:line 138
   at Raven.Database.Indexing.DefaultBackgroundTaskExecuter.ExecuteAllBuffered[T](WorkContext context, IList`1 source, Action`1 action) in e:\dev\ravensrc\ravendb-build-2851\Raven.Database\Indexing\DefaultBackgroundTaskExecuter.cs:line 95
   at Raven.Database.Indexing.SimpleIndex.<>c__DisplayClass1a.<IndexDocuments>b__b(RavenIndexWriter indexWriter, Analyzer analyzer, IndexingWorkStats stats) in e:\dev\ravensrc\ravendb-build-2851\Raven.Database\Indexing\SimpleIndex.cs:line 156
   at Raven.Database.Indexing.Index.Write(Func`4 action) in e:\dev\ravensrc\ravendb-build-2851\Raven.Database\Indexing\Index.cs:line 387, Document: , Action: Write}
			 * 
			 */
			Assert.True(results.Count() == 1);
		}

		private string BuildString(char element, int desiredLength)
		{
			var sb = new StringBuilder(desiredLength);
			Enumerable.Range(0, desiredLength - 1).ForEach((_) => sb.Append(element));
			return sb.ToString();
		}

		[Theory]
		[InlineData(WithoutCollationIndex.StaticIndexName)]
		[InlineData(EtCollationIndex.StaticIndexName)]
		[InlineData(DeCollationIndex.StaticIndexName)]
		public void IndexWithEtCollationAnalyzer_DetermineFailingIndex(string indexName)
		{
			//ARRANGE
			var store = InitStore();
			var session = store.OpenSession();

			DatabaseStatistics stats = null;


			var contact1 = new Contact()
			{
				FirstName = BuildString('a', 280)
			};

			session.Store(contact1);
			session.SaveChanges();

			//ACT
			var results = session.Advanced.DocumentQuery<Contact>(indexName).OrderBy(x => x.FirstName)
				.WaitForNonStaleResults()
				.ToList();

			stats = store.DatabaseCommands.GetStatistics();

			//ASSERT

			Console.WriteLine("Field content:{0}, Errors:\n {1}", contact1.FirstName, string.Join("\n", stats.Errors.ToList()));

			Assert.True(stats.Errors.All(x => x.IndexName != indexName));
		}
	}

	public class WithoutCollationIndex : AbstractIndexCreationTask<RavenDB_2113.Contact>
	{
		public const string StaticIndexName = "WithoutCollationIndex";

		public WithoutCollationIndex()
		{
			Map = contacts =>
				from contact in contacts
				select new
				{
					contact.FirstName,
				};

			//Analyzers.Add(x => x.FirstName, "Raven.Database.Indexing.Collation.Cultures.EtCollationAnalyzer, Raven.Database");
			Sort(x => x.FirstName, SortOptions.String);
		}

		public override string IndexName
		{
			get { return StaticIndexName; }
		}
	}

	public class EtCollationIndex : AbstractIndexCreationTask<RavenDB_2113.Contact>
	{
		public const string StaticIndexName = "EtCollationIndex";

		public EtCollationIndex()
		{
			Map = contacts =>
				from contact in contacts
				select new
				{
					contact.FirstName,
				};

			Analyzers.Add(x => x.FirstName, typeof(EtCollationAnalyzer).AssemblyQualifiedName);
			Sort(x => x.FirstName, SortOptions.String);
		}

		public override string IndexName
		{
			get { return StaticIndexName; }
		}
	}

	public class DeCollationIndex : AbstractIndexCreationTask<RavenDB_2113.Contact>
	{
		public const string StaticIndexName = "DeCollationIndex";

		public DeCollationIndex()
		{
			Map = contacts =>
				from contact in contacts
				select new
				{
					contact.FirstName,
				};

			Analyzers.Add(x => x.FirstName, typeof(DeCollationAnalyzer).AssemblyQualifiedName);
			Sort(x => x.FirstName, SortOptions.String);
		}

		public override string IndexName
		{
			get { return StaticIndexName; }
		}
	}
}