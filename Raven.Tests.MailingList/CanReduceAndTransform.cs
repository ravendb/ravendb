// -----------------------------------------------------------------------
//  <copyright file="CanReduceAndTransform.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class CanReduceAndTransform : RavenTestBase
	{
		private class Personnel
		{
			public string Id { get; set; }
			public string LastName { get; set; }
		}

		public class PersonnelAll
			: AbstractMultiMapIndexCreationTask<PersonnelAll.Mapping>
		{
			public PersonnelAll()
			{
				AddMap<Personnel>(personnel =>
								  from person in personnel
								  select new Mapping
								  {
									  Id = person.Id,
									  LastName = person.LastName,
									  Roles = null
								  });
				AddMap<PersonnelRole>(roles =>
									  from role in roles
									  select new Mapping
									  {
										  Id = role.PersonnelId,
										  LastName = null,
										  Roles = new[] { role.RoleId }
									  });

				Reduce = results => from result in results
									group result by result.Id
										into g
										select new Mapping
										{
											Id = g.Select(a => a.Id).FirstOrDefault(a => a != null),
											LastName = g.Select(a => a.LastName).FirstOrDefault(a => a != null),
											Roles = g.SelectMany(a => a.Roles)
										};
			}

			public class Mapping
			{
				public string Id { get; set; }
				public string LastName { get; set; }
				public IEnumerable<string> Roles { get; set; }
			}
		}


		private class PersonnelRole
		{
			public string Id { get; set; }
			public string PersonnelId { get; set; }
			public string RoleId { get; set; }
		}

		[Fact]
		public void WillTransform()
		{
			using (EmbeddableDocumentStore store = NewDocumentStore())
			{
				using (IDocumentSession session = store.OpenSession())
				{
					var personnel = new Personnel { LastName = "Ayende" };
					session.Store(personnel);

					var role = new PersonnelRole { PersonnelId = personnel.Id, RoleId = "Roles/Administrator" };
					session.Store(role);

					session.SaveChanges();
				}

				new PersonnelAll().Execute(store);
				new PersonnelTransformer().Execute(store);

				using (IDocumentSession session = store.OpenSession())
				{
					List<PersonnelTransformer.Result> results = session.Query<PersonnelAll.Mapping, PersonnelAll>()
																	   .Customize(
																		   customization => customization.WaitForNonStaleResults())
																	   .TransformWith<PersonnelTransformer, PersonnelTransformer.Result>()
																	   .ToList();

					Assert.Empty(store.DatabaseCommands.GetStatistics().Errors);
					Assert.Equal("Ayende", results.First().FullName);
				}
			}
		}

		public class PersonnelTransformer
			: AbstractTransformerCreationTask<PersonnelAll.Mapping>
		{
			public PersonnelTransformer()
			{
				TransformResults = results =>
								   from result in results
								   select new Result
								   {
									   Id = result.Id,
									   FullName = result.LastName
								   };
			}

			public class Result
			{
				public string Id { get; set; }
				public string FullName { get; set; }
			}
		}
	}
}
