// -----------------------------------------------------------------------
//  <copyright file="TransformationsUnitTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class TransformationsUnitTest : RavenTest
    {
        [Fact]
        public void TestTransformations()
        {
            using (var documentStore = NewRemoteDocumentStore(databaseName: "Demo"))
            {
                new MiniMemberTransformer().Execute(documentStore.DatabaseCommands.ForDatabase("Demo"),
                    documentStore.Conventions);

                using (var session = documentStore.OpenSession("Demo"))
                {
                    session.Store(new Member {Name = "Matt", Foo = "aaa", Bar = "bbb"});
                    session.Store(new Member {Name = "John", Foo = "aaa", Bar = "bbb"});
                    session.Store(new Member {Name = "Fred", Foo = "aaa", Bar = "bbb"});
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession("Demo"))
                {
                    var members = session
                        .Query<Member>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<MiniMemberTransformer, MiniMember>()
                        .Take(512)
                        .ToList();

                }
            }
        }

        public class Member
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Foo { get; set; }
            public string Bar { get; set; }
        }

        public class MiniMember
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class MiniMemberTransformer : AbstractTransformerCreationTask<Member>
        {
            public MiniMemberTransformer()
            {
                TransformResults = members =>
                    from member in members
                    select new MiniMember
                    {
                        Id = member.Id,
                        Name = member.Name
                    };
            }
        }
 
    }
}