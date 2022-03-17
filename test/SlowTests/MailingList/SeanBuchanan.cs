// -----------------------------------------------------------------------
//  <copyright file="SeanBuchanan.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class SeanBuchanan : RavenTestBase
    {
        public SeanBuchanan(ITestOutputHelper output) : base(output)
        {
        }

        private class Consultant : INamedDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int YearsOfService { get; set; }
        }

        private interface INamedDocument
        {
            string Id { get; set; }

            string Name { get; set; }
        }

        private class Skill : INamedDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Proficiency
        {
            public string Id { get; set; }
            public DenormalizedReference<Consultant> Consultant { get; set; }
            public DenormalizedReference<Skill> Skill { get; set; }
            public string SkillLevel { get; set; }
        }

        private class DenormalizedReference<T> where T : INamedDocument
        {
            public string Id { get; set; }
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
                    var skill1 = new Skill { Name = "C#" };
                    var skill2 = new Skill { Name = "SQL" };
                    var consultant1 = new Consultant { Name = "Subha", YearsOfService = 6 };
                    var consultant2 = new Consultant { Name = "Tom", YearsOfService = 5 };

                    session.Store(skill1);
                    session.Store(skill2);
                    session.Store(consultant1);
                    session.Store(consultant2);

                    var proficiency1 = new Proficiency
                    {
                        Consultant = consultant1,
                        Skill = skill1,
                        SkillLevel = "Expert"
                    };

                    session.Store(proficiency1);
                    session.SaveChanges();

                    var proficiencies = session.Query<Proficiency, Proficiencies_ConsultantId>()
                                               .Customize(o => o.WaitForNonStaleResults())
                                               .Where(o => o.Consultant.Id == consultant1.Id)
                                               .ToList();

                    Assert.Equal("Subha", proficiencies.Single().Consultant.Name);
                }

                //Block2
                using (var session = store.OpenSession())
                {
                    var consultant1 = session.Load<Consultant>("consultants/1-A");

                    //Here I am changing the name of one consultant from "Subha" to "Subhashini".
                    //A denormalized reference to this name exists in the Proficiency class. After this update, I will need to sync the denormalized reference.
                    //As I am changing a Consultant document, I would not expect my index to need updating because the index is on the Proficiency collection.
                    consultant1.Name = "Subhashini";
                    session.SaveChanges();


                    //Block1
                    //This block of code simply lists the names of the consultants in the Proficiencies collection. Since I have not synced the collection
                    //yet, I expect the consultant name to still be "Subha."
                    var proficiencies = session.Query<Proficiency>("Proficiencies/ConsultantId")
                                      .Customize(o => o.WaitForNonStaleResults())
                                      .Where(o => o.Consultant.Id == consultant1.Id)
                                      .ToList();

                    Assert.Equal("Subha", proficiencies.Single().Consultant.Name);
                }

                Indexes.WaitForIndexing(store);
                //I use this patch to update the consultant name to "Subha" in the Proficiencies collection.
                store.Operations.Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = "FROM INDEX 'Proficiencies/ConsultantId' WHERE Consultant_Id = 'consultants/1-A' UPDATE { this.Consultant.Name = 'Subhashini'; }"
                })).WaitForCompletion(TimeSpan.FromSeconds(15));

                //Here, I again list the name of the consultant in the Proficiencies collection and expect it to be "Subhashini".
                using (var session = store.OpenSession())
                {
                    //Block2
                    var proficiencies = session.Query<Proficiency>("Proficiencies/ConsultantId")
                                               .Customize(o => o.WaitForNonStaleResults())
                                               .Where(o => o.Consultant.Id == "consultants/1-A")
                                               .ToList();

                    Assert.Equal("Subhashini", proficiencies.Single().Consultant.Name);
                }
            }
        }
    }
}
