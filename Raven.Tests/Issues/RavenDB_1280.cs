using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Xunit;
using Xunit.Sdk;
using Raven.Client;

namespace Raven.Tests.Indexes
{
	public class RavenDB_1280 : RavenTest
	{
		//[Fact]
		[TimeBombedFact(2013, 12, 31)]
		public void Referenced_Docs_Are_Indexed_During_Heavy_Writing()
		{
			const int iterations = 8000;

			using (var documentStore = NewRemoteDocumentStore(requestedStorage:"esent"))
			{
				new EmailIndex().Execute(documentStore);

				Parallel.For(0, iterations, i =>
				{
					using (var session = documentStore.OpenSession())
					{
						session.Store(new EmailDocument {Id = "Emails/"+ i,To = "root@localhost", From = "nobody@localhost", Subject = "Subject" + i});
						session.SaveChanges();
					}

					using (var session = documentStore.OpenSession())
					{
						session.Store(new EmailText { Id = "Emails/" + i + "/text", Body = "MessageBody" + i });
						session.SaveChanges();
					}
				});

				WaitForIndexing(documentStore);

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<EmailIndexDoc, EmailIndex>().Count(e => e.Body.StartsWith("MessageBody"));
				    try
				    {
                        Assert.Equal(iterations, results);
				    }
				    catch (Exception ex)
				    {
                        var missingDocs = session.Query<EmailIndexDoc, EmailIndex>().AsProjection<EmailIndexDoc>()
                                                                                    .Where(e => !e.Body.StartsWith("MessageBody"))
                                                                                    .ToList();
                        Console.WriteLine(string.Join(", ", missingDocs.Select(doc => doc.Id).ToArray()));
				        Console.WriteLine(ex.Message);
				        throw;
				    }
				}
			}
		}

        [Fact]
        public void CanHandleMultipleMissingDocumentsInMultipleIndexes()
        {
            using (var store = NewDocumentStore())
            {
                var indexDefinition = new EmailIndex().CreateIndexDefinition();

                for (int i = 0; i < 4; i++)
                {
                    store.DatabaseCommands.PutIndex("email" + i, indexDefinition);
                    
                }

                using (var session = store.OpenSession())
                {
                    session.Store(entity: new EmailDocument { });
                    session.Store(entity: new EmailDocument { });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
            }
        }

		public class EmailIndex : AbstractIndexCreationTask<EmailDocument, EmailIndexDoc>
		{
			public EmailIndex()
			{
				Map =
					emails => from email in emails
							let text = LoadDocument<EmailText>(email.Id + "/text") 				
							select new
									{
										email.To,
										email.From,
										email.Subject,
										Body = text == null ? null : text.Body
									};
			}
		}

		public class EmailDocument
		{
			public string Id { get; set; }
			public string To { get; set; }
			public string From { get; set; }
			public string Subject { get; set; }
		}

		public class EmailText
		{
			public string Id { get; set; }
			public string Body { get; set; }
		}

		public class EmailIndexDoc
		{
			public string Id { get; set; }
			public string To { get; set; }
			public string From { get; set; }
			public string Subject { get; set; }
			public string Body { get; set; }			
		}
	}
}
