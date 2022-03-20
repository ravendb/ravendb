using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class EtagIssue : RavenTestBase
    {
        public EtagIssue(ITestOutputHelper output) : base(output)
        {
        }

        #region Domain

        private enum CredentialsStatus
        {
            Inactive,
            Active
        }

        private enum PublishStatus
        {
            Unpublished,
            Published
        }

        private enum RelationStatus
        {
            Active,
            Inactive
        }

        private class Credentials
        {
            public CredentialsStatus Status { get; set; }
        }

        private sealed class HasManager : Relation
        {
            internal HasManager(UserProfile user)
                : base(user)
            {
            }

            private HasManager()
            {
            }
        }

        private class InnovationProfile : Profile
        {
            public OrganizationProfile Owner { get; set; }
            public PublishStatus Status { get; set; }
        }

        private sealed class ManagerOf : Relation
        {
            internal ManagerOf(OrganizationProfile organization)
                : base(organization)
            {
            }

            private ManagerOf()
            {
            }
        }

        private class OrganizationProfile : Profile
        {
        }

        private class Profile
        {
            public Profile()
            {
                Relations = new List<Relation>();
            }

            public string Id { get; set; }

            public string Name { get; set; }
            public string Slug { get; set; }

            public List<Relation> Relations { get; set; }
        }

        private abstract class Relation
        {
            protected Relation(
                Profile to,
                string subType = null)
                : this()
            {
                To = to;
                SubType = subType;
            }

            protected internal Relation()
            {
                Type = GetType().Name;
            }

            public string Type { get; protected internal set; }
            public string SubType { get; protected internal set; }

            public Profile To { get; private set; }

            public RelationStatus Status { get; set; }
            protected internal string ActivationToken { get; set; }
        }

        private class UserManagedItem
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public string OwnerId { get; set; }
            public bool IsManagedDirectly { get; set; }

            public PublishStatus PublishStatus { get; set; }
            public RelationStatus RelationStatus { get; set; }
        }

        private class UserProfile : Profile
        {
        }

        #endregion

        #region Index

        private class UserProfileIndex : AbstractMultiMapIndexCreationTask<UserProfileIndex.Result>
        {
            public UserProfileIndex()
            {
                AddMap<UserProfile>(docs =>
                    from doc in docs
                    select new ReduceResult
                    {
                        Id = doc.Id,
                        Slug = doc.Slug,
                        Status = LoadDocument<Credentials>(doc.Id + "/credentials").Status,
                        DateUpdated = MetadataFor(doc).Value<DateTime>(Constants.Documents.Metadata.LastModified),
                        ManagedItems =
                            from r in doc.Relations
                            where r.Type == "CaseManagerOf"
                            let profile = LoadDocument<InnovationProfile>(r.To.Id)
                            select new UserManagedItem
                            {
                                Id = r.To.Id,
                                Name = r.To.Name,
                                OwnerId = profile.Owner.Id,
                                IsManagedDirectly = true,
                                PublishStatus = profile.Status,
                                RelationStatus = r.Status,
                            },
                        ManagedItems_RelationStatus = null
                    });

                AddMap<InnovationProfile>(docs =>
                    from doc in docs
                    let caseManagerId = doc.Relations.SingleOrDefault(r => r.Type == "HasCaseManager").To.Id
                    let org = LoadDocument<OrganizationProfile>(doc.Owner.Id)
                    // count org managers as innovation managers if they are not case managers already
                    from r in org.Relations.Where(r => r.Type == "HasManager" && r.SubType == null && r.To.Id != caseManagerId)
                    select new ReduceResult
                    {
                        Id = r.To.Id,
                        Slug = null,
                        Status = CredentialsStatus.Inactive,
                        DateUpdated = DateTime.MinValue,
                        ManagedItems = new[]
                        {
                            new UserManagedItem
                            {
                                Id = doc.Id,
                                Name = doc.Name,
                                OwnerId = doc.Owner.Id,
                                IsManagedDirectly = false,
                                PublishStatus = doc.Status,
                                RelationStatus = r.Status
                            }
                        },
                        ManagedItems_RelationStatus = null
                    });

                Reduce = results =>
                    from r in results
                    group r by r.Id
                    into g
                    let u = g.First(it => it.DateUpdated != DateTime.MinValue)
                    select new ReduceResult
                    {
                        Id = g.Key,
                        Slug = u.Slug,
                        Status = u.Status,
                        DateUpdated = u.DateUpdated,
                        ManagedItems = g.SelectMany(it => it.ManagedItems).Distinct(),
                        ManagedItems_RelationStatus = g.SelectMany(it => it.ManagedItems).Select(it => it.RelationStatus)
                    };

                Index(r => r.Id, FieldIndexing.Exact);
                Store(r => r.Id, FieldStorage.Yes);
                Store(r => r.Slug, FieldStorage.Yes);
                Store(r => r.DateUpdated, FieldStorage.Yes);
                Store(r => r.ManagedItems, FieldStorage.Yes);
            }

            public class ReduceResult : Result
            {
                public IEnumerable<RelationStatus> ManagedItems_RelationStatus { get; set; }
            }

            public class Result
            {
                public string Id { get; set; }
                public string Slug { get; set; }
                public CredentialsStatus Status { get; set; }
                public DateTime DateUpdated { get; set; }

                public IEnumerable<UserManagedItem> ManagedItems { get; set; }
            }
        }

        #endregion

        [Fact]
        public void ScriptedPatchShouldNotResultInConcurrencyExceptionForNewlyInsertedDocument()
        {
            using (var store = GetDocumentStore())
            {
                new UserProfileIndex().Execute(store);

                OrganizationProfile[] organizations =
                {
                    new OrganizationProfile {Id = "organizations/1", Name = "University of California"},
                    new OrganizationProfile {Id = "organizations/2", Name = "University of Stanford"}
                };

                var orgAdmin1 = new UserProfile { Id = "users/1" };
                var orgAdmin2 = new UserProfile { Id = "users/2" };

                var orgAdmins = new[]
                {
                    new {User = orgAdmin1, Org = organizations[0]},
                    new {User = orgAdmin2, Org = organizations[1]}
                };

                Store(store, organizations);
                Store(store, new[] { orgAdmin1, orgAdmin2 });

                Indexes.WaitForIndexing(store);

                IEnumerable<ICommandData> patches = orgAdmins.SelectMany(orgAdmin =>
                    Establish(orgAdmin.User, new ManagerOf(orgAdmin.Org),
                        orgAdmin.Org, new HasManager(orgAdmin.User)));

                using (var commands = store.Commands())
                {
                    dynamic user1 = commands.Get("users/1");
                    dynamic user2 = commands.Get("users/2");

                    var relations1 = user1.Relations;
                    var relations2 = user2.Relations;

                    Assert.Equal(0, relations1.Length);
                    Assert.Equal(0, relations2.Length);

                    commands.Batch(patches.ToList());

                    user1 = commands.Get("users/1");
                    user2 = commands.Get("users/2");

                    relations1 = user1.Relations;
                    relations2 = user2.Relations;

                    Assert.Equal(1, relations1.Length);
                    Assert.Equal(1, relations2.Length);
                }
            }
        }

        private IEnumerable<PatchCommandData> Establish(UserProfile user, ManagerOf managerOf,
            OrganizationProfile organization, HasManager hasManager)
        {
            const string patchScript = "var _this = this;" +
                                       "function addRelation(clrType, relation, thisArg) {" +
                                       " relation['$type'] = clrType; " +
                                       "   (thisArg || _this).Relations.push(relation);" +
                                       "}" + "addRelation(args.relationClrType, args.relation);";

            yield return new PatchCommandData(user.Id, null, new PatchRequest
            {
                Script = patchScript,
                Values = new Dictionary<string, object>
                {
                    {"relation", managerOf},
                    {"relationClrType", ClrType(managerOf.GetType())}
                }
            }, null);

            yield return new PatchCommandData(organization.Id, null, new PatchRequest
            {
                Script = patchScript,
                Values = new Dictionary<string, object>
                {
                    {"relation", hasManager},
                    {"relationClrType", ClrType(hasManager.GetType())}
                }
            }, null);
        }

        private static string ClrType(Type t)
        {
            return string.Concat(t.FullName, ", ", t.Assembly.GetName().Name);
        }

        private void Store(IDocumentStore store, params object[] objs)
        {
            using (IDocumentSession session = store.OpenSession())
            {
                session.Advanced.UseOptimisticConcurrency = true;

                foreach (object obj in objs)
                    session.Store(obj);
                session.SaveChanges();
            }
        }
    }
}
