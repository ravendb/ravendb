// -----------------------------------------------------------------------
//  <copyright file="CanPatchADocumentThatContainsBytes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class CanPatchADocumentThatContainsBytes : RavenTest
    {
        [Fact]
        public void DocumentWithBytes()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        EmailEncrypted = new byte[] {1, 2, 3, 4, 5, 6},
                        Skills = new Collection<UserSkill>
                        {
                            new UserSkill {SkillId = 1, IsPrimary = true},
                        }
                    });
                    session.SaveChanges();
                }

                new PrimarySkills().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    store.DatabaseCommands.UpdateByIndex("PrimarySkills", new IndexQuery {Query = session.Query<PrimarySkills.Result, PrimarySkills>().Where(result => result.SkillId == 1).ToString()}, new ScriptedPatchRequest
                    {
                        Script = @"
for (var i = 0; i < this.Skills.$values.length; i++) {
    this.Skills.$values[i].IsPrimary = false
}
"
                    }).WaitForCompletion();

                    var user = session.Load<User>(1);
                    Assert.False(user.Skills.Single().IsPrimary);
                }
            }   
        }

        public class User
        {
            public int Id { get; set; }
            public byte[] EmailEncrypted { get; set; }
            public ICollection<UserSkill> Skills { get; set; }
        }

        public class UserSkill
        {
            public int SkillId { get; set; }
            public bool IsPrimary { get; set; }
        }

        public class Skill
        {
            public int Id { get; set; }
        }

        public class PrimarySkills : AbstractIndexCreationTask<User, PrimarySkills.Result>
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