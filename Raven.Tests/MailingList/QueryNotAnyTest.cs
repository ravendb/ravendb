using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class QueryNotAny : RavenTest
	{
		[Fact]
		public void Query_NotAny_WithEnumComparison()
		{
			using (GetNewServer())
			using (var documentStore = new DocumentStore()
			{
				Url = "http://localhost:8079",
				Conventions = { DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites }
			}.Initialize())
			{
				new Users_ByRoles().Execute(documentStore);
				using (var session = documentStore.OpenSession())
				{
					session.Store(new User { Id = 1, Roles = new List<Role> { new Role { Type = UserType.Contract }, new Role { Type = UserType.Developer } } });
					session.Store(new User { Id = 2, Roles = new List<Role> { new Role { Type = UserType.Permanent }, new Role { Type = UserType.Developer } } });
					session.Store(new User { Id = 3, Roles = new List<Role> { new Role { Type = UserType.SeniorDeveloper }, new Role { Type = UserType.Manager } } });
					session.Store(new User { Id = 4, Roles = new List<Role> { new Role { Type = UserType.Contract }, new Role { Type = UserType.SeniorDeveloper } } });
					session.Store(new User { Id = 5, Roles = new List<Role> { new Role { Type = UserType.Permanent }, new Role { Type = UserType.Manager } } });
					session.Store(new User { Id = 6, Roles = new List<Role> { new Role { Type = UserType.Contract }, new Role { Type = UserType.Developer } } });
					session.SaveChanges();

					var nonContractEmployees =
						session.Query<Users_ByRoles.Result, Users_ByRoles>()
							.Customize(x => x.WaitForNonStaleResults())
							.Where(x => x.RoleType != UserType.Contract)
							.As<User>()
							.ToList();

					Assert.Equal(3, nonContractEmployees.Count());
				}
			}
		}

		public class Users_ByRoles : AbstractIndexCreationTask<User, Users_ByRoles.Result>
		{
			public class Result
			{
				public UserType RoleType { get; set; }
			}

			public Users_ByRoles()
			{
				Map = users =>
					  from user in users
					  select new
					  {
						  RoleType = user.Roles.Select(x => x.Type)
					  };
			}
		}
		public class User
		{
			public int Id { get; set; }
			public List<Role> Roles { get; set; }
		}

		public class Role
		{
			public UserType Type { get; set; }
		}

		public enum UserType
		{
			Manager,
			Permanent,
			Contract,
			Developer,
			SeniorDeveloper
		}
	}
}