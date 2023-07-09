using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Queries;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20634 : RavenTestBase
{
    public RavenDB_20634(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void IdOfEmbeddedObjectIsNotWrappedIntoJavaScriptIdFunction()
    {
        using (var store = GetDocumentStore(options: new Options
               {
                   ModifyDocumentStore = ss =>
                   {
                       ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                       {
                           CustomizeJsonSerializer = s => { s.ContractResolver = new CamelCasePropertyNamesContractResolver(); }
                       };
                       ss.Conventions.PropertyNameConverter = mi => $"{Char.ToLower(mi.Name[0])}{mi.Name.Substring(1)}";
                       ss.Conventions.ShouldApplyPropertyNameConverter = info => true;
                   }
               }))
        {
            using (var session = store.OpenSession())
            {
                var user = new MyUser { Id = "users/1", UserName = "john" };

                session.Store(user);

                var workspace = new Workspace { Id = "workspaces/1", Invites = new List<Invite> { new Invite { Id = "invites/1", Invitee = user.Id } } };

                session.Store(workspace);

                session.SaveChanges();
            }
 
            using (var session = store.OpenSession())
            {
                var query = (from workspace in session.Query<Workspace>()
                    let invite = workspace.Invites.FirstOrDefault(x => x.Id == "invites/1")
                    let inviter = RavenQuery.Load<MyUser>(invite.Invitee)
                    select new PendingInvite { Id = invite.Id, Created = invite.Created, Inviter = inviter.UserName });
                var result = query.ToList();
                Assert.NotNull(result);
                Assert.Equal(1, result.Count);
                Assert.Equal("john", result[0].Inviter);
                Assert.Equal("invites/1", result[0].Id);
            }
        }
    }

    private class MyUser
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string UserName { get; set; }
        public int Age { get; set; }

        public List<Rating> HasRated { get; set; }

        public class Rating
        {
            public string Movie { get; set; }
            public int Score { get; set; }
        }
    }

    private class Workspace
    {
        public string Id { get; set; }
        public List<Invite> Invites { get; set; } = new List<Invite>();
    }

    private class Invite
    {
        public string Id { get; set; }
        public DateTime Created { get; set; } = DateTime.UtcNow;
        public string Invitee { get; set; }
    }

    private class PendingInvite
    {
        public string Id { get; set; }
        public DateTime Created { get; set; }
        public string Inviter { get; set; }
    }
}
