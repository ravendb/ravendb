// -----------------------------------------------------------------------
//  <copyright file="Jon.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Jon : RavenTestBase
    {
        [Fact]
        public void CanQueryUsingDistintOnIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var generatedRoles = JsonConvert.DeserializeObject<List<Role>>("[{\"Id\":null,\"Name\":\"Name1\",\"Permissions\":[0]},{\"Id\":null,\"Name\":\"Name2\",\"Permissions\":[1]},{\"Id\":null,\"Name\":\"Name3\",\"Permissions\":[2]},{\"Id\":null,\"Name\":\"Name4\",\"Permissions\":[0,1]},{\"Id\":null,\"Name\":\"Name5\",\"Permissions\":[0,2]},{\"Id\":null,\"Name\":\"Name6\",\"Permissions\":[0,1,2]}]");

                    foreach (var role in generatedRoles)
                    {
                        session.Store(role);
                    }
 
                    var generatedUsers = JsonConvert.DeserializeObject<List<User>>("[{\"Id\":null,\"Username\":\"Username1\",\"Password\":\"Password1\",\"Roles\":[{\"Id\":\"roles/6\",\"Name\":\"Name6\"}]},{\"Id\":null,\"Username\":\"Username2\",\"Password\":\"Password2\",\"Roles\":[{\"Id\":\"roles/1\",\"Name\":\"Name1\"},{\"Id\":\"roles/2\",\"Name\":\"Name2\"},{\"Id\":\"roles/3\",\"Name\":\"Name3\"}]},{\"Id\":null,\"Username\":\"Username3\",\"Password\":\"Password3\",\"Roles\":[{\"Id\":\"roles/1\",\"Name\":\"Name1\"},{\"Id\":\"roles/2\",\"Name\":\"Name2\"},{\"Id\":\"roles/3\",\"Name\":\"Name3\"},{\"Id\":\"roles/6\",\"Name\":\"Name6\"}]},{\"Id\":null,\"Username\":\"Username4\",\"Password\":\"Password4\",\"Roles\":[{\"Id\":\"roles/5\",\"Name\":\"Name5\"}]},{\"Id\":null,\"Username\":\"Username5\",\"Password\":\"Password5\",\"Roles\":[{\"Id\":\"roles/1\",\"Name\":\"Name1\"},{\"Id\":\"roles/3\",\"Name\":\"Name3\"}]},{\"Id\":null,\"Username\":\"Username6\",\"Password\":\"Password6\",\"Roles\":[{\"Id\":\"roles/4\",\"Name\":\"Name4\"}]},{\"Id\":null,\"Username\":\"Username7\",\"Password\":\"Password7\",\"Roles\":[{\"Id\":\"roles/1\",\"Name\":\"Name1\"},{\"Id\":\"roles/2\",\"Name\":\"Name2\"}]}]");

                    foreach (var user in generatedUsers)
                    {
                        session.Store(user);
                    }

                    session.SaveChanges();
                }

                new PermissionsByUser().Execute(store);
                new PermissionsByUserTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    var userWithPermissionses = session.Query<UserWithPermissions, PermissionsByUser>().Customize(x =>
                        x.WaitForNonStaleResults()).TransformWith<PermissionsByUserTransformer, UserWithPermissions>().ToList();
                    Assert.NotEmpty(userWithPermissionses);
                }
            }
        }

        private class PermissionsByUser : AbstractIndexCreationTask<User>
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
            }
        }

        private class PermissionsByUserTransformer : AbstractTransformerCreationTask<User>
        {
            public PermissionsByUserTransformer()
            {
                TransformResults = users => from user in users
                                            let roles = LoadDocument<Role>(user.Roles.Select(x => x.Id))
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

        private class User
        {
            public string Id { get; set; }
            public string Username { get; set; }
            public string Password { get; set; }
            public IEnumerable<RoleReference> Roles { get; set; }
        }

        private class UserWithPermissions : User
        {
            public IEnumerable<Permissions> Permissions { get; set; }
        }

        private class RoleReference
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Role
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public IEnumerable<Permissions> Permissions { get; set; }
        }

        private enum Permissions
        {
            Read,
            Write,
            Delete
        }
    }
}
