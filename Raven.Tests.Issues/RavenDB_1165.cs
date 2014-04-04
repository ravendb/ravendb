using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Document;
using System.Diagnostics;
using System;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class CanTransformAndStream : RavenTestBase
	{
		public class Personnel
		{
			public string Id { get; set; }

			public string FirstName { get; set; }
			public string MiddleName { get; set; }
			public string LastName { get; set; }

			public string OrganizationalUnitId { get; set; }
			public string FinancialUnitId { get; set; }
			public string PersonnelTypeId { get; set; }
		}


		public class PersonnelAchievementsMatrixAll
		   : AbstractMultiMapIndexCreationTask<PersonnelAchievementsMatrixAll.Mapping>
		{
			public class AchievementMapping
			{
				public string Id { get; set; }
				public string AchievementId { get; set; }
				public bool IsAcquired { get; set; }
				public DateTime? DateFrom { get; set; }
				public DateTime? DateTo { get; set; }
			}

			public class Mapping
			{
				public string Id { get; set; }
				public string LastName { get; set; }
			}

			public PersonnelAchievementsMatrixAll()
			{
				AddMap<Personnel>(personnel =>
					from person in personnel
					select new Mapping
					{
						Id = person.Id,
						LastName = person.LastName
					});

				Reduce = results => from result in results
									group result by result.Id
										into g
										select new Mapping
										{
											Id = g.Select(a => a.Id).FirstOrDefault(a => a != null),
											LastName = g.Select(a => a.LastName).FirstOrDefault(a => a != null)
										};
			}
		}

		public class PersonnelMatrixTransformer
			: AbstractTransformerCreationTask<PersonnelAchievementsMatrixAll.Mapping>
		{
			public class PersonnelMatrix
			{
				public string PersonnelId { get; set; }
				public string FullName { get; set; }
			}
			public PersonnelMatrixTransformer()
			{
				TransformResults = results =>
					from result in results
					let person = LoadDocument<Personnel>(result.Id)
					select new PersonnelMatrix
					{
						PersonnelId = result.Id,
						FullName = string.Join(" ",
								new string[]{ 
                                person.FirstName, 
                                person.MiddleName, 
                                person.LastName
                            }.Where(a => !string.IsNullOrEmpty(a))
						)
					};
			}
		}

		[Fact]
		public void StreamAll()
		{
			using (var store = this.NewDocumentStore())
			{
				// Add some users
				using (var session = store.OpenSession())
				{
					var personnel = new Personnel { FirstName = "Rahien", LastName = "Ayende" };
					session.Store(personnel);

					var personnel2 = new Personnel { FirstName = "Komo", LastName = "Diablo" };
					session.Store(personnel2);

					session.SaveChanges();
				}

				new PersonnelAchievementsMatrixAll().Execute(store);
				new PersonnelMatrixTransformer().Execute(store);

				using (var session = store.OpenSession())
				{
					var waitForNonStale = session.Query<PersonnelAchievementsMatrixAll.Mapping, PersonnelAchievementsMatrixAll>().Customize(customization => customization.WaitForNonStaleResults());
					Debug.WriteLine(waitForNonStale.ToArray().Count());

					var personnel = session.Query<PersonnelAchievementsMatrixAll.Mapping, PersonnelAchievementsMatrixAll>();
					var query = personnel.TransformWith<PersonnelMatrixTransformer, PersonnelMatrixTransformer.PersonnelMatrix>();
					var stream = session.Advanced.Stream(query);

					foreach (var person in query)
					{
						Debug.WriteLine(person.FullName);
					}

					while (stream.MoveNext())
					{
						Assert.NotNull(stream.Current.Document.FullName);
					}

				}
			}
		}
	}
}
