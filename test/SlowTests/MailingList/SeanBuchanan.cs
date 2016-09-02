// -----------------------------------------------------------------------
//  <copyright file="SeanBuchanan.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class SeanBuchanan : RavenTestBase
    {
        private class Consultant : INamedDocument
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int YearsOfService { get; set; }
        }

        private interface INamedDocument
        {
            int Id { get; set; }

            string Name { get; set; }
        }

        private class Skill : INamedDocument
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        private class Proficiency
        {
            public int Id { get; set; }
            public DenormalizedReference<Consultant> Consultant { get; set; }
            public DenormalizedReference<Skill> Skill { get; set; }
            public string SkillLevel { get; set; }
        }

        private class DenormalizedReference<T> where T : INamedDocument
        {
            public int Id { get; set; }
            public string Name { get; set; }

            public static implicit operator DenormalizedReference<T>(T doc)
            {
                return new DenormalizedReference<T>
                {
                    Id = doc.Id,
                    Name = doc.Name
                };
            }
        }

        private class Proficiencies_ConsultantId : AbstractIndexCreationTask<Proficiency>
        {
            public Proficiencies_ConsultantId()
            {
                Map = proficiencies => proficiencies.Select(proficiency => new { Consultant_Id = proficiency.Consultant.Id });
            }
        }

        [Fact]
        public void PatchShouldWorkCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Proficiencies_ConsultantId());

                //Write the test data to the database.
                using (var session = store.OpenSession())
                {
                    var skill1 = new Skill { Id = 1, Name = "C#" };
                    var skill2 = new Skill { Id = 2, Name = "SQL" };
                    var consultant1 = new Consultant { Id = 1, Name = "Subha", YearsOfService = 6 };
                    var consultant2 = new Consultant { Id = 2, Name = "Tom", YearsOfService = 5 };
                    var proficiency1 = new Proficiency
                    {
                        Id = 1,
                        Consultant = consultant1,
                        Skill = skill1,
                        SkillLevel = "Expert"
                    };

                    session.Store(skill1);
                    session.Store(skill2);
                    session.Store(consultant1);
                    session.Store(consultant2);
                    session.Store(proficiency1);
                    session.SaveChanges();

                    var proficiencies = session.Query<Proficiency, Proficiencies_ConsultantId>()
                                               .Customize(o => o.WaitForNonStaleResultsAsOfLastWrite())
                                               .Where(o => o.Consultant.Id == 1)
                                               .ToList();

                    Assert.Equal("Subha", proficiencies.Single().Consultant.Name);
                }

                //Block2
                using (var session = store.OpenSession())
                {
                    var consultant1 = session.Load<Consultant>(1);

                    //Here I am changing the name of one consultant from "Subha" to "Subhashini".
                    //A denormalized reference to this name exists in the Proficiency class. After this update, I will need to sync the denormalized reference.
                    //As I am changing a Consultant document, I would not expect my index to need updating because the index is on the Proficiency collection.
                    consultant1.Name = "Subhashini";
                    session.SaveChanges();


                    //Block1
                    //This block of code simply lists the names of the consultants in the Proficiencies collection. Since I have not synced the collection
                    //yet, I expect the consultant name to still be "Subha."
                    var proficiencies = session.Query<Proficiency>("Proficiencies/ConsultantId")
                                      .Customize(o => o.WaitForNonStaleResultsAsOfLastWrite())
                                      .Where(o => o.Consultant.Id == 1)
                                      .ToList();

                    Assert.Equal("Subha", proficiencies.Single().Consultant.Name);
                }

                WaitForIndexing(store);
                //I use this patch to update the consultant name to "Subha" in the Proficiencies collection.
                store.DatabaseCommands.UpdateByIndex("Proficiencies/ConsultantId",
                    new IndexQuery
                    {
                        Query = "Consultant_Id:1"
                    },
                    new PatchRequest
                    {
                        Script = "this.Consultant.Name = 'Subhashini';"
                    },
                    options: null).WaitForCompletion();

                //Here, I again list the name of the consultant in the Proficiencies collection and expect it to be "Subhashini".
                using (var session = store.OpenSession())
                {
                    //Block2
                    var proficiencies = session.Query<Proficiency>("Proficiencies/ConsultantId")
                                               .Customize(o => o.WaitForNonStaleResultsAsOfLastWrite())
                                               .Where(o => o.Consultant.Id == 1).ToList();

                    Assert.Equal("Subhashini", proficiencies.Single().Consultant.Name);
                }
            }
        }
    }
}
