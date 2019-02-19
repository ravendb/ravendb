using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Xunit;

namespace SlowTests.Bugs
{
    public class AvoidRaceConditionWhenWeLoadTheDataNotPatched : RavenTestBase
    {
        [Fact]
        public void GetReturnsFilteredResults()
        {
            using (var store = GetDocumentStore())
            {
                var users = new[]
                {
                    new UserProfile {Id = "users/1"},
                    new UserProfile {Id = "users/2"},
                    new UserProfile {Id = "users/3"}
                };

                var innovations = new[]
                {
                    new InnovationProfile {Id = "innovations/1"},
                    new InnovationProfile {Id = "innovations/2"},
                    new InnovationProfile {Id = "innovations/3"},
                    new InnovationProfile {Id = "innovations/4"},
                    new InnovationProfile {Id = "innovations/5"},
                };

                store.ExecuteIndex(new UserProfileIndex());
          
                using (var session = store.OpenSession())
                {
                    foreach (var obj in innovations)
                        session.Store(obj);
                    foreach (var obj1 in users)
                        session.Store(obj1);
                    session.SaveChanges();
                }

                var patches = users.SelectMany(
                    user => innovations.SelectMany(
                        inno => Establish(inno, user)));
                Commit(patches.ToArray(), store);

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    var user = session.Load<UserProfile>("users/1");

                    if (!user.Relations.Any())
                    {
                        throw new Exception("User has no relations");
                    }
                }
            }
        }

        public IEnumerable<PatchCommandData> Establish(InnovationProfile edge1Profile, UserProfile edgeNProfile)
        {
            Relation edge1 = new HasCaseManager(edgeNProfile);
            Relation edgeN = new CaseManagerOf(edge1Profile);

            yield return new PatchCommandData(edge1Profile.Id, null, new PatchRequest
            {
                Script = "addRelation(args.relationClrType, args.relation);",
                Values = new Dictionary<string, object>
                {
                    {"relation", edge1},
                    {"relationClrType", edge1.GetType().AssemblyQualifiedName},
                    {"otherSideRelationType", edgeN.Type},
                    {"otherSideRelationSubType", edgeN.SubType}
                }
            }, null);

            yield return new PatchCommandData(edgeNProfile.Id, null, new PatchRequest
            {
                Script = "addRelation(args.relationClrType, args.relation);",
                Values = new Dictionary<string, object>
                {
                    {"relation", edgeN},
                    {"relationClrType", edgeN.GetType().AssemblyQualifiedName}
                }
            }, null);
        }

        public void Commit(PatchCommandData[] patches, IDocumentStore store)
        {
            var mergedPatchCommands = patches
                .GroupBy(cmd => cmd.Id)
                .Select(g =>
                {
                    string docKey = g.Select(it => it.Id).First();

                    int index = 0;
                    foreach (var cmd in g)
                    {
                        // rename parameters
                        var originalValues = cmd.Patch.Values;
                        cmd.Patch.Values = new Dictionary<string, object>();
                        foreach (var kvValue in originalValues)
                        {
                            string newKey = kvValue.Key + index;
                            cmd.Patch.Values.Add(newKey, kvValue.Value);
                            cmd.Patch.Script = Regex.Replace(cmd.Patch.Script, @"\b" + kvValue.Key + @"\b", newKey);
                        }
                        index++;
                    }

                    var scriptedPatchRequest = new PatchRequest
                    {
                        Script = "var _this = this;" +
                                 "function addRelation(clrType, relation, thisArg) {" +
                                 "    relation['$type'] = clrType; " + 
                                 "   (thisArg || _this).Relations.push(relation);" +
                                 "}" +
                                 string.Join("\n\n", g.Select(cmd => cmd.Patch.Script)),
                        Values = g.SelectMany(cmd => cmd.Patch.Values).ToDictionary(kv => kv.Key, kv => kv.Value)
                    };

                    return new PatchOperation(docKey, null, scriptedPatchRequest);
                }).ToArray();

            foreach (var patchCommandData in mergedPatchCommands)
            {
                if (store.Operations.Send(patchCommandData) != PatchStatus.Patched)
                    throw new InvalidOperationException("Some patches failed");
            }
        }

        public class UserProfileIndex : AbstractIndexCreationTask<UserProfile, UserProfileIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public CredentialsStatus Status { get; set; }
                public IEnumerable<string> ManagedItems { get; set; }
            }

            public UserProfileIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new Result
                    {
                        Id = doc.Id,
                        Status = LoadDocument<Credentials>(doc.Id + "/credentials").Status,
                        ManagedItems =
                            from r in doc.Relations
                            where r.Type == "CaseManagerOf"
                            let profile = LoadDocument<InnovationProfile>(r.To.Id)
                            select r.To.Id
                    };
            }
        }

        public class Profile
        {
            public string Id { get; set; }

            public virtual string Name { get; set; }

            public List<Relation> Relations { get; protected set; }

            public Profile()
            {
                Relations = new List<Relation>();
            }
        }

        public abstract class Relation
        {
            public string Type { get; protected internal set; }
            public string SubType { get; protected internal set; }

            public Profile To { get; private set; }

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
        }

        public sealed class CaseManagerOf : Relation
        {
            internal CaseManagerOf(InnovationProfile innovation)
                : base(innovation)
            { }

            private CaseManagerOf() { }
        }

        public sealed class HasCaseManager : Relation
        {
            internal HasCaseManager(UserProfile user)
                : base(user)
            { }

            private HasCaseManager() { }
        }

        public class InnovationProfile : Profile { }

        public class UserProfile : Profile { }

        public enum CredentialsStatus { Inactive }

        public class Credentials
        {
            public string Id { get; set; }
            public CredentialsStatus Status { get; set; }
        }
    }
}
