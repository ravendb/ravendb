using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs.MapRedue
{
	public class MapReduceIndex : RavenTest
	{
		private readonly String[] m_documentIds;

		public MapReduceIndex()
		{
			m_documentIds = new String[5]
			{
				"One",
				"Two",
				"Three",
				"Four ",
				"Five"
			};
		}

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.MaxNumberOfParallelIndexTasks = 1;
		}

		[Fact]
		public void MapReduceIndexTest()
		{
			using (var store = NewDocumentStore())
			{
				new VersionedDocuments().Execute(store);

				RemoveAllDocuments(store);

				// index should return no document
				Assert.Equal(0, NumberOfDocumentsInDbByIndex(store));

				// insert documents for all ids
				using (IDocumentSession session = store.OpenSession())
				{
					for (int index = 0;
					     index < m_documentIds.Length;
					     index++)
					{
						InserDocumentIntoDb(session,
						                    m_documentIds[index], Convert.ToByte(index + 1));
					}

					session.SaveChanges();
				}

				// index should return number of document equal to number of inserted
				Assert.Equal(m_documentIds.Length,
				             NumberOfDocumentsInDbByIndex(store));

				RemoveAllDocuments(store);

				// index should return no document
				Assert.Equal(0, NumberOfDocumentsInDbByIndex(store));

				// insert documents one document with the same id as one of documents inserted before
				using (IDocumentSession session = store.OpenSession())
				{
					InserDocumentIntoDb(session, m_documentIds[0], 1);

					session.SaveChanges();
				}

				// index should return one document
				Assert.Equal(1, NumberOfDocumentsInDbByIndex(store));
			}
		}

		private void RemoveAllDocuments(IDocumentStore aStore)
		{
			aStore.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery {Query = "Tag:Documents"}, allowStale: false);
		}

		private void InserDocumentIntoDb(IDocumentSession aSession,
		                                 String aDocumentId, Byte aVersionCount)
		{
			var versions = new List<VersionedDocument>(aVersionCount);
			for (int index = 0; index < aVersionCount; index++)
			{
				versions.Add(new VersionedDocument
				{
					Id = aDocumentId,
					Version = Convert.ToUInt32(index
					                           + 1),
					Data = String.Format("Data for version {0}", index + 1)
				});
			}

			var document = new Document
			{
				Id = aDocumentId,
				Versions = versions.ToArray()
			};

			aSession.Store(document);
		}

		private int NumberOfDocumentsInDbByIndex(IDocumentStore aStore)
		{
			using (IDocumentSession session = aStore.OpenSession())
			{
				IRavenQueryable<DocumentView> query =
					session.Query<Document, VersionedDocuments>()
						.Customize(aCustomization => aCustomization.WaitForNonStaleResultsAsOfNow(TimeSpan.FromMinutes(10)))
						.AsProjection<DocumentView>();

				foreach (DocumentView document in query)
				{
					Debug.WriteLine(String.Format("Document {0} v.{1}", document.Id, document.Version));
				}

				return query.Count();
			}
		}
	}
}