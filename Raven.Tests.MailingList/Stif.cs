using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Stif : RavenTest
	{
		[DataContract]
		public class MyDoc
		{
			[DataMember]
			public Guid Id { get; set; }
			[DataMember]
			public string Message { get; set; }
			[DataMember]
			public LogTypeEnum Type { get; set; }
		}

		[DataContract]
		public class Context
		{
			[DataMember]
			public string Key { get; set; }

			[DataMember]
			public string Value { get; set; }
		}

		[Flags]
		public enum LogTypeEnum
		{
			Usage = 1,
			Changes = 2,
			Error = 4,
			Performance = 8,
			Authentication = 16,
			ProductError = 32,
			ProductUsage = 64,
			NewsItem = 128,
			UserActivity = 256,
			All = 511
		}

		public class MyDocIndex : AbstractIndexCreationTask<MyDoc, MyDocIndex.MyDocResult>
		{
			public class MyDocResult
			{
				public string Message { get; set; }
				public bool IsChange { get; set; }
			}

			public MyDocIndex()
			{
				Map = docs => docs.Select(
					lm => new MyDocResult()
					{
						Message = lm.Message,
						IsChange = (lm.Type & LogTypeEnum.Changes) == LogTypeEnum.Changes
					});

				Analyzers.Add(lm => lm.Message, "Lucene.Net.Analysis.Standard.StandardAnalyzer, Lucene.Net");
			}
		}

		[Fact]
		public void GetDummyDoc()
		{
			using (var documentStore = new EmbeddableDocumentStore {RunInMemory = true})
			{
				documentStore.Conventions.SaveEnumsAsIntegers = true;
				documentStore.Conventions.DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites;
				documentStore.Initialize();
				new MyDocIndex().Execute(documentStore);

				using (IDocumentSession documentSession = documentStore.OpenSession())
				{
					documentSession.Store(new MyDoc {Id = new Guid(0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0), Message = "some dummy message"});
					documentSession.SaveChanges();
				}

				using (IDocumentSession documentSession = documentStore.OpenSession())
				{
					MyDoc docFetched = documentSession.Load<MyDoc>(new Guid(0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0)); //returns an object
					Debug.WriteLine(string.Format("found {0}", docFetched.Id));

					Assert.NotNull(docFetched);

					List<MyDoc> docs = documentSession.Query<MyDoc>().ToList(); //returns an empty list
					Debug.WriteLine(string.Format("found {0} docs", docs.Count));

					Assert.Equal(1, docs.Count);

				}

			}
		}
	}
}