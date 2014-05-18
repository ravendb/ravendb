using System;
using System.Collections.Generic;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class NicolasGarfinkiel : RavenTest
	{
		public class LaboratoryTrial
		{
			public DateTimeOffset CreatedDate { get; set; }

			public DateTimeOffset DeliveryDate { get; set; }

			public DateTimeOffset LastModifiedDate { get; set; }

			public string Satus { get; set; }

			public Patient Patient { get; set; }

		}

		public class Patient
		{

			public string Firstname { get; set; }

			public string Lastname { get; set; }

			public Dictionary<string, string> IdCards { get; set; }

		}

		protected override void CreateDefaultIndexes(IDocumentStore documentStore)
		{
		}

		[Fact]
		public void CanQueryDynamically()
		{
			using (var store = NewDocumentStore())
			{
				store.DocumentDatabase.Configuration.MaxNumberOfParallelIndexTasks = 1;
				store.DatabaseCommands.PutIndex("Foos/TestIndex", new IndexDefinition()
				{
					Map =
						@"from doc in docs.LaboratoryTrials
			select new
			{
				_ = doc.Patient.IdCards.Select((Func<dynamic,dynamic>)(x => new Field(x.Key, x.Value, Field.Store.NO, Field.Index.ANALYZED_NO_NORMS)))
			}"
				}, true);

				using(var session = store.OpenSession())
				{
					session.Store(new LaboratoryTrial
					{
						CreatedDate = DateTimeOffset.Now,
						DeliveryDate = DateTimeOffset.Now,
						LastModifiedDate = DateTimeOffset.Now,
						Satus = "H",
						Patient = new Patient
						{
							Firstname = "Ha",
							Lastname = "Dr",
							IdCards = new Dictionary<string, string>
							{
								{"Read", "Yes"},
							}
						}
					});
					session.SaveChanges();
				}

				using(var session=store.OpenSession())
				{
                    var laboratoryTrials = session.Advanced.DocumentQuery<LaboratoryTrial>("Foos/TestIndex")
						.WaitForNonStaleResultsAsOfLastWrite(TimeSpan.FromHours(1))
						.WhereEquals("Read", "Yes")
						.ToList();
					Assert.NotEmpty(laboratoryTrials);
				}
			}
		}
	}
}