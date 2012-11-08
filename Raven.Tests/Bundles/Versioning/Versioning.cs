//-----------------------------------------------------------------------
// <copyright file="Versioning.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;
using Raven.Client.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Versioning
{
	public class Versioning : VersioningTest
	{
		[Fact]
		public void Will_automatically_set_metadata()
		{
			var company = new Company {Name = "Company Name"};
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var company2 = session.Load<Company>(company.Id);
				var metadata = session.Advanced.GetMetadataFor(company2);
				Assert.Equal("Current", metadata.Value<string>("Raven-Document-Revision-Status"));
				Assert.Equal(1, metadata.Value<int>("Raven-Document-Revision"));
			}
		}

		[Fact]
		public void Can_exclude_entities_from_versioning()
		{
			var user = new User {Name = "User Name"};
			var comment = new Comment {Name = "foo"};
			using (var session = documentStore.OpenSession())
			{
				session.Store(user);
				session.Store(comment);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				Assert.Null(session.Load<User>(user.Id + "/revisions/1"));
				Assert.Null(session.Load<Comment>(comment.Id + "/revisions/1"));
			}

			using (var sesion = documentStore.OpenSession())
			{
				var metadata = sesion.Advanced.GetMetadataFor(sesion.Load<User>(user.Id));
				Assert.Null(metadata.Value<string>("Raven-Document-Revision-Status"));
				Assert.Equal(0, metadata.Value<int>("Raven-Document-Revision"));
			}
		}

		[Fact]
		public void Will_automatically_update_metadata_on_next_insert()
		{
			var company = new Company {Name = "Company Name"};
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
				company.Name = "Hibernating Rhinos";
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var company2 = session.Load<Company>(company.Id);
				var metadata = session.Advanced.GetMetadataFor(company2);
				Assert.Equal("Current", metadata.Value<string>("Raven-Document-Revision-Status"));
				Assert.Equal(2, metadata.Value<int>("Raven-Document-Revision"));
			}
		}

		[Fact]
		public void Will_automatically_craete_duplicate_on_first_insert()
		{
			var company = new Company {Name = "Company Name"};
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var company2 = session.Load<Company>(company.Id + "/revisions/1");
				var metadata = session.Advanced.GetMetadataFor(company2);
				Assert.Equal(company.Name, company2.Name);
				Assert.Equal("Historical", metadata.Value<string>("Raven-Document-Revision-Status"));
			}
		}

		[Fact]
		public void Will_automatically_craete_duplicate_on_next_insert()
		{
			var company = new Company {Name = "Company Name"};
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
				Assert.Equal(1, session.Advanced.GetMetadataFor(company).Value<int>("Raven-Document-Revision"));
			}
			using (var session = documentStore.OpenSession())
			{
				var company3 = session.Load<Company>(company.Id);
				company3.Name = "Hibernating Rhinos";
				session.SaveChanges();
				Assert.Equal(2, session.Advanced.GetMetadataFor(company3).Value<int>("Raven-Document-Revision"));
			}
			using (var session = documentStore.OpenSession())
			{
				var company2 = session.Load<Company>(company.Id + "/revisions/1");
				var metadata = session.Advanced.GetMetadataFor(company2);
				Assert.Equal("Company Name", company2.Name);
				Assert.Equal("Historical", metadata.Value<string>("Raven-Document-Revision-Status"));
				Assert.Null(metadata.Value<string>("Raven-Document-Parent-Revision"));

				company2 = session.Load<Company>(company.Id + "/revisions/2");
				metadata = session.Advanced.GetMetadataFor(company2);
				Assert.Equal("Hibernating Rhinos", company2.Name);
				Assert.Equal("Historical", metadata.Value<string>("Raven-Document-Revision-Status"));
				Assert.Equal("companies/1/revisions/1", metadata.Value<string>("Raven-Document-Parent-Revision"));
			}
		}

		[Fact]
		public void Can_get_all_revisions()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
				Assert.Equal(1, session.Advanced.GetMetadataFor(company).Value<int>("Raven-Document-Revision"));
			}
			using (var session = documentStore.OpenSession())
			{
				var company3 = session.Load<Company>(company.Id);
				company3.Name = "Hibernating Rhinos";
				session.SaveChanges();
				Assert.Equal(2, session.Advanced.GetMetadataFor(company3).Value<int>("Raven-Document-Revision"));
			}
			using (var session = documentStore.OpenSession())
			{
				var companiesRevisions = session.Advanced.GetRevisionsFor<Company>(company.Id, 0, 25);
				Assert.Equal("Company Name", companiesRevisions[0].Name);
				Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
			}
		}

		[Fact]
		public void Will_delete_old_revisions()
		{
			var company = new Company {Name = "Company #1"};
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
				for (int i = 0; i < 10; i++)
				{
					company.Name = "Company #" + i + 2;
					session.SaveChanges();
				}
			}

			using (var session = documentStore.OpenSession())
			{
				for (int i = 1; i < 7; i++)
				{
					Assert.Null(session.Load<Company>(company.Id + "/revisions/" + i));
				}

				for (int i = 7; i < 12; i++)
				{
					Assert.NotNull(session.Load<Company>(company.Id + "/revisions/" + i));
				}

				for (int i = 12; i < 21; i++)
				{
					Assert.Null(session.Load<Company>(company.Id + "/revisions/" + i));
				}
			}
		}

		[Fact]
		public void Will_not_delete_revisions_if_parent_exists()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<object>("companies/1/revisions/1");
				Assert.NotNull(doc);

				session.Advanced.Defer(new DeleteCommandData
				{
					Key = "companies/1/revisions/1",
					TransactionInformation = new TransactionInformation()
				});

				Assert.Throws<InvalidOperationException>(() => session.SaveChanges());
			}
		}

		[Fact]
		public void Will_delete_revisions_if_version_is_deleted()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<object>("companies/1/revisions/1");
				var comp = session.Load<object>("companies/1");
				Assert.NotNull(doc);

				session.Delete(comp);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<object>("companies/1/revisions/1");
				Assert.NotNull(doc);

				session.Advanced.Defer(new DeleteCommandData
				{
					Key = "companies/1/revisions/1",
					TransactionInformation = new TransactionInformation()
				});
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<object>("companies/1/revisions/1");
				Assert.Null(doc);
			}
		}

		[Fact]
		public void Will_delete_child_revisions_if_purge_is_true()
		{
			using(var session = documentStore.OpenSession())
			{
				session.Store(new VersioningConfiguration
				{
					Exclude = false,
					PurgeOnDelete = true,
					Id = "Raven/Versioning/Companies"
				});

				session.SaveChanges();
			}

			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<object>("companies/1");
				Assert.NotNull(doc);

				session.Delete(doc);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<object>("companies/1/revisions/1");
				Assert.Null(doc);
			}
		}

		[Fact]
		public void Will_not_delete_child_revisions_if_purge_is_false()
		{
			using (var session = documentStore.OpenSession())
			{
				session.Store(new VersioningConfiguration
				{
					Exclude = false,
					PurgeOnDelete = false,
					Id = "Raven/Versioning/Companies"
				});

				session.SaveChanges();
			}

			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<object>("companies/1");
				Assert.NotNull(doc);

				session.Delete(doc);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var doc = session.Load<object>("companies/1/revisions/1");
				Assert.NotNull(doc);
			}
		}

		#region Nested type: Comment

		public class Comment
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		#endregion

		#region Nested type: User

		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		#endregion
	}

	public class Company
	{
		public string Name { get; set; }
		public string Id { get; set; }
	}
}
