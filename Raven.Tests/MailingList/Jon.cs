// -----------------------------------------------------------------------
//  <copyright file="Jon.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using FizzWare.NBuilder;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Jon : RavenTest
	{
		[Fact]
		public void CanQueryUsingDistintOnIndex()
		{
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var generatedRoles = Builder<Role>
						.CreateListOfSize(6)
						.All().With(x => x.Id = null)
						.TheFirst(1).With(x => x.Permissions = new List<Permissions> { Permissions.Read })
						.TheNext(1).With(x => x.Permissions = new List<Permissions> { Permissions.Write })
						.TheNext(1).With(x => x.Permissions = new List<Permissions> { Permissions.Delete })
						.TheNext(1).With(x => x.Permissions = new List<Permissions> { Permissions.Read, Permissions.Write })
						.TheNext(1).With(x => x.Permissions = new List<Permissions> { Permissions.Read, Permissions.Delete })
						.TheNext(1).With(x => x.Permissions = new List<Permissions> { Permissions.Read, Permissions.Write, Permissions.Delete })
						.Build();

					foreach (var role in generatedRoles)
					{
						session.Store(role);
					}
					var roleRefs = generatedRoles.Select(x => new RoleReference { Id = x.Id, Name = x.Name });

					var generatedUsers = Builder<User>
						.CreateListOfSize(7)
						.All().With(x => x.Id = null)
						.TheFirst(1).With(x => x.Roles = new List<RoleReference> { roleRefs.ElementAt(5) })
						.TheNext(1).With(x => x.Roles = new List<RoleReference> { roleRefs.ElementAt(0), roleRefs.ElementAt(1), roleRefs.ElementAt(2) })
						.TheNext(1).With(x => x.Roles = new List<RoleReference> { roleRefs.ElementAt(0), roleRefs.ElementAt(1), roleRefs.ElementAt(2), roleRefs.ElementAt(5) })
						.TheNext(1).With(x => x.Roles = new List<RoleReference> { roleRefs.ElementAt(4) })
						.TheNext(1).With(x => x.Roles = new List<RoleReference> { roleRefs.ElementAt(0), roleRefs.ElementAt(2) })
						.TheNext(1).With(x => x.Roles = new List<RoleReference> { roleRefs.ElementAt(3) })
						.TheNext(1).With(x => x.Roles = new List<RoleReference> { roleRefs.ElementAt(0), roleRefs.ElementAt(1) })
						.Build();

					foreach (var user in generatedUsers)
					{
						session.Store(user);
					}

					session.SaveChanges();
				}

				new PermissionsByUser().Execute(store);

				using(var session = store.OpenSession())
				{
					var userWithPermissionses = session.Query<UserWithPermissions, PermissionsByUser>().Customize(x => 
						x.WaitForNonStaleResults()).ToList();
					Assert.NotEmpty(userWithPermissionses);
				}
			}
		}

		public class PermissionsByUser : AbstractIndexCreationTask<User, UserWithPermissions>
		{
			public override string IndexName
			{
				get
				{
					return "Users/PermissionsByUser";
				}
			}
			public PermissionsByUser()
			{
				Map = users => from user in users
							   from role in user.Roles
							   select new { role.Id };

				TransformResults = (database, users) => from user in users
														let roles = database.Load<Role>(user.Roles.Select(x => x.Id))
														select new
														{
															Id = user.Id,
															Username = user.Username,
															Password = user.Password,
															Roles = user.Roles,
															Permissions = roles.SelectMany(x => x.Permissions)//.Distinct()
														};
			}
		}

		public class User
		{
			public string Id { get; set; }
			public string Username { get; set; }
			public string Password { get; set; }
			public IEnumerable<RoleReference> Roles { get; set; }
		}
		public class UserWithPermissions : User
		{
			public IEnumerable<Permissions> Permissions { get; set; }
		}

		public class RoleReference
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
		public class Role
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public IEnumerable<Permissions> Permissions { get; set; }
		}

		public enum Permissions
		{
			Read,
			Write,
			Delete
		}
	}
}