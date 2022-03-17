// -----------------------------------------------------------------------
//  <copyright file="CanPatchADocumentThatContainsBytes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CanPatchADocumentThatContainsBytes : RavenTestBase
    {
        public CanPatchADocumentThatContainsBytes(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DocumentWithBytes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        EmailEncrypted = new byte[] { 1, 2, 3, 4, 5, 6 },
                        Skills = new Collection<UserSkill>
                        {
                            new UserSkill {SkillId = 1, IsPrimary = true},
                        }
                    });
                    session.SaveChanges();
                }

                new PrimarySkills().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var index = GetIndexQuery(session.Query<PrimarySkills.Result, PrimarySkills>()
                        .Where(result => result.SkillId == 1));

                    index.Query += @" update { 
for (var i = 0; i < this.Skills.length; i++) {
    this.Skills[i].IsPrimary = false
}
}";
                    var operation = store.Operations.Send(new PatchByQueryOperation(index));

                    operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                    var user = session.Load<User>("Users/1-A");
                    Assert.False(user.Skills.Single().IsPrimary);
                }
            }
        }

        private static IndexQuery GetIndexQuery<T>(IQueryable<T> queryable)
        {
            var inspector = (IRavenQueryInspector)queryable;
            return inspector.GetIndexQuery(isAsync: false);
        }

        private class User
        {
            //public int Id { get; set; }
            public byte[] EmailEncrypted { get; set; }
            public ICollection<UserSkill> Skills { get; set; }
        }

        private class UserSkill
        {
            public int SkillId { get; set; }
            public bool IsPrimary { get; set; }
        }

        private class Skill
        {
            public int Id { get; set; }
        }

        private class PrimarySkills : AbstractIndexCreationTask<User, PrimarySkills.Result>
        {

            public class Result
            {
                public int SkillId { get; set; }

                public bool IsPrimary { get; set; }
            }

            public PrimarySkills()
            {
                Map = users => from u in users
                               from s in u.Skills
                               where s.IsPrimary
                               select new
                               {
                                   s.SkillId,
                                   s.IsPrimary
                               };

                Store(x => x.SkillId, FieldStorage.Yes);
                Store(x => x.IsPrimary, FieldStorage.Yes);
            }
        }
    }
}
