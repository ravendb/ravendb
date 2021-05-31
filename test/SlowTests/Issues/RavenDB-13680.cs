using System;
using System.Collections.Generic;
using FastTests;
using FastTests.Server.Basic.Entities;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure.Entities;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_13680 : RavenTestBase
    {
        public RavenDB_13680(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public void CanSelectLoadsInsideLetClause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Product = "products/1"
                            },
                            new OrderLine
                            {
                                Product = "products/2"
                            }
                        }
                    });
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Product = "products/3"
                            },
                            new OrderLine
                            {
                                Product = "products/4"
                            }
                        }
                    });

                    session.Store(new Product
                    {
                        Name = "a"
                    }, "products/1");
                    session.Store(new Product
                    {
                        Name = "b"
                    }, "products/2");
                    session.Store(new Product
                    {
                        Name = "c"
                    }, "products/3");
                    session.Store(new Product
                    {
                        Name = "d"
                    }, "products/4");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = from doc in session.Query<Order>()
                            let p = doc.Lines.Select(y => RavenQuery.Load<Product>(y.Product))
                            select new
                            {
                                p
                            };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(doc) {
	var p = doc.Lines.map(function(y){return load(y.Product);});
	return { p : p };
}
from 'Orders' as doc select output(doc)", q.ToString());

                    var result = q.ToList();
                    Assert.Equal(2, result.Count);

                    var products = result[0].p.ToList();
                    Assert.Equal(2, products.Count);
                    Assert.Equal("a", products[0].Name);
                    Assert.Equal("b", products[1].Name);

                    products = result[1].p.ToList();
                    Assert.Equal(2, products.Count);
                    Assert.Equal("c", products[0].Name);
                    Assert.Equal("d", products[1].Name);

                }

            }
        }


        [Fact]
        public void CanSelectLoadsInsideLetClause_complex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new PersonProfileDocument(), "PeopleProfileDocuments/1-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var id = "PeopleProfileDocuments/1-A";

                    var result =
                               (from doc in session.Query<PersonProfileDocument>()
                                where (doc.Id == id)
                                let person = RavenQuery.Load<PersonDocument>(doc.Person.DocumentId)
                                let occupations = doc.UncoveredOccupationInfos.Select(y => RavenQuery.Load<PersonOccupationDocument>(y.Occupation.DocumentId))
                                let projects = doc.UncoveredProjectInfos.Select(y => RavenQuery.Load<ProjectDocument>(y.Project.DocumentId))
                                let publications = doc.UncoveredPublicationInfos.Select(y => RavenQuery.Load<PublicationDocument>(y.Publication.DocumentId))
                                let classifications = publications.Select(y => RavenQuery.Load<ClassifierItemDocument>(y.Classification.DocumentId))
                                let mentorshipsRunning = doc.UncoveredRunningMentorshipInfos.Select(y => RavenQuery.Load<MentorshipDocument>(y.Mentorship.DocumentId))
                                let mentorshipsFinalized = doc.UncoveredFinalizedMentorshipInfos.Select(y => RavenQuery.Load<MentorshipDocument>(y.Mentorship.DocumentId))
                                let mentorshipsDissertation = doc.UncoveredDissertationInfos.Select(y => RavenQuery.Load<MentorshipDocument>(y.Mentorship.DocumentId))
                                select new PersonProfileTransformerInfo()
                                {
                                    IndexId = doc.Id.Split('/', StringSplitOptions.None).Last(),
                                    IsPublic = doc.IsPublic,
                                    IsConfirmed = doc.IsConfirmedEst,
                                    IsConfirmedEng = doc.IsConfirmedEng,
                                    HasPublicDataEst = doc.UserHasPublicDataEst,
                                    HasPublicDataEng = doc.UserHasPublicDataEng,
                                    PersonId = doc.Person.Id.ToString(),
                                    PersonName = person.Name,
                                    Occupations = doc.UncoveredOccupationInfos.Select(occ => new PersonProfileTransformerOccupationInfo()
                                    {
                                        DocumentId = occ.Occupation.Id.ToString(),
                                        Period = RavenQuery.Load<PersonOccupationDocument>(occ.Occupation.DocumentId).Period,
                                        IsActive = RavenQuery.Load<PersonOccupationDocument>(occ.Occupation.DocumentId).IsActive,
                                        IsPublicEst = occ.IsPublic,
                                        IsPublicEng = occ.IsEngPublic,
                                        DisplayString = RavenQuery.Load<PersonOccupationDocument>(occ.Occupation.DocumentId).DisplayString,
                                        DisplayStringEng = RavenQuery.Load<PersonOccupationDocument>(occ.Occupation.DocumentId).DisplayStringEng,
                                    }),
                                    Projects = doc.UncoveredProjectInfos.Select(pr => new PersonProfileTransformerProjectInfo()
                                    {
                                        DocumentId = pr.Project.Id.ToString(),
                                        IsPublicEst = pr.IsPublic,
                                        IsPublicEng = pr.IsEngPublic,
                                        DisplayString = RavenQuery.Load<ProjectDocument>(pr.Project.DocumentId).DisplayInfoEst,
                                        DisplayStringEng = RavenQuery.Load<ProjectDocument>(pr.Project.DocumentId).DisplayInfoEng,
                                        IsActive = RavenQuery.Load<ProjectDocument>(pr.Project.DocumentId).IsActive,
                                        EndDate = RavenQuery.Load<ProjectDocument>(pr.Project.DocumentId).General.EndDate,
                                        PeriodIsActive = RavenQuery.Load<ProjectDocument>(pr.Project.DocumentId).General.ProjectPeriod.IsActive
                                    }),
                                    Publications = doc.UncoveredPublicationInfos.Select(pr => new PersonProfileTransformerPublicationInfo()
                                    {
                                        DocumentId = pr.Publication.Id.ToString(),
                                        IsPublicEst = pr.IsPublic,
                                        IsPublicEng = pr.IsEngPublic,
                                        DisplayString = RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).DisplayInfoHtml,
                                        DisplayStringEng = RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).DisplayInfoHtmlEng,

                                        PublishingYear = RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).PublishingYear,
                                        IsActive = RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).IsActive,
                                        ClassificationCode = (RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).Classification != null ? RavenQuery.Load<ClassifierItemView>(RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).Classification.DocumentId).Code : ""),
                                        ClassificationCodeName = (RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).Classification != null ? RavenQuery.Load<ClassifierItemDocument>(RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).Classification.DocumentId).FullName : ""),
                                        ClassificationCodeNameEng = (RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).Classification != null ? RavenQuery.Load<ClassifierItemDocument>(RavenQuery.Load<PublicationDocument>(pr.Publication.DocumentId).Classification.DocumentId).FullNameEng : "")
                                    }),
                                    Mentorships = doc.UncoveredRunningMentorshipInfos.Select(ms => new PersonProfileTransformerMentorshipInfo()
                                    {
                                        DocumentId = ms.Mentorship.Id.ToString(),
                                        IsPublicEst = ms.IsPublic,
                                        IsPublicEng = ms.IsEngPublic,
                                        DisplayString = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).DisplayInfoEst,
                                        DisplayStringEng = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).DisplayInfoEng,
                                        IsActive = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).IsActive,
                                        Degree = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).Degree,
                                        DefenceYear = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).DefenceYear
                                    }),
                                    MentorshipsFinalized = doc.UncoveredFinalizedMentorshipInfos.Select(ms => new PersonProfileTransformerMentorshipInfo()
                                    {
                                        DocumentId = ms.Mentorship.Id.ToString(),
                                        IsPublicEst = ms.IsPublic,
                                        IsPublicEng = ms.IsEngPublic,
                                        DisplayString = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).DisplayInfoEst,
                                        DisplayStringEng = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).DisplayInfoEng,
                                        IsActive = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).IsActive,
                                        Degree = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).Degree,
                                        DefenceYear = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).DefenceYear
                                    }),
                                    MentorshipsDissertation = doc.UncoveredDissertationInfos.Select(ms => new PersonProfileTransformerMentorshipInfo()
                                    {
                                        DocumentId = ms.Mentorship.Id.ToString(),
                                        IsPublicEst = ms.IsPublic,
                                        IsPublicEng = ms.IsEngPublic,
                                        DisplayString = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).DisplayInfoEst,
                                        DisplayStringEng = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).DisplayInfoEng,
                                        IsActive = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).IsActive,
                                        Degree = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).Degree,
                                        DefenceYear = RavenQuery.Load<MentorshipDocument>(ms.Mentorship.DocumentId).DefenceYear
                                    })
                                }).FirstOrDefault();

                    Assert.NotNull(result);
                }
            }
        }

        private class PersonProfileDocument
        {
            public string Id { get; set; }

            public Person Person { get; set; }

            public bool IsPublic { get; set; }

            public bool IsConfirmedEst { get; set; }

            public bool IsConfirmedEng { get; set; }

            public bool UserHasPublicDataEst { get; set; }

            public bool UserHasPublicDataEng { get; set; }

            public List<UncoveredOccupationInfo> UncoveredOccupationInfos { get; set; }

            public List<UncoveredProjectInfo> UncoveredProjectInfos { get; set; }

            public List<UncoveredPublicationInfo> UncoveredPublicationInfos { get; set; }

            public List<UncoveredRunningMentorshipInfo> UncoveredRunningMentorshipInfos { get; set; }

            public List<UncoveredRunningMentorshipInfo> UncoveredFinalizedMentorshipInfos { get; set; }

            public List<UncoveredRunningMentorshipInfo> UncoveredDissertationInfos { get; set; }

        }

        private class PersonDocument
        {
            public string Name { get; set; }
        }

        private class Person
        {
            public string Id { get; set; }

            public string DocumentId { get; set; }
        }

        private class Occupation
        {
            public string Id { get; set; }

            public string DocumentId { get; set; }
        }

        private class PersonOccupationDocument
        {
            public string Period { get; set; }

            public bool IsActive { get; set; }

            public string DisplayString { get; set; }

            public string DisplayStringEng { get; set; }


        }

        private class UncoveredOccupationInfo
        {
            public Occupation Occupation { get; set; }

            public bool IsPublic { get; set; }

            public bool IsEngPublic { get; set; }
        }

        private class UncoveredPublicationInfo
        {
            public Publication Publication { get; set; }

            public bool IsPublic { get; set; }

            public bool IsEngPublic { get; set; }
        }

        private class UncoveredProjectInfo
        {
            public Project Project { get; set; }

            public bool IsPublic { get; set; }

            public bool IsEngPublic { get; set; }

        }

        private class ProjectDocument
        {
            public string DisplayInfoEst { get; set; }

            public string DisplayInfoEng { get; set; }

            public bool IsActive { get; set; }

            public General General { get; set; }

        }


        private class General
        {
            public DateTime EndDate { get; set; }

            public ProjectPeriod ProjectPeriod { get; set; }

        }

        private class ProjectPeriod
        {
            public bool IsActive { get; set; }
        }

        private class Project
        {
            public string DocumentId { get; set; }

            public string Id { get; set; }

        }

        private class Publication
        {
            public string Id { get; set; }

            public string DocumentId { get; set; }
        }


        private class PublicationDocument
        {
            public Classification Classification { get; set; }

            public int PublishingYear { get; set; }

            public bool IsActive { get; set; }

            public string DisplayInfoHtml { get; set; }

            public string DisplayInfoHtmlEng { get; set; }

        }

        private class ClassifierItemDocument
        {
            public string FullNameEng { get; set; }

            public string FullName { get; set; }
        }


        private class Classification
        {
            public string DocumentId { get; set; }
        }

        private class UncoveredRunningMentorshipInfo
        {
            public Mentorship Mentorship { get; set; }

            public bool IsPublic { get; set; }

            public bool IsEngPublic { get; set; }

        }

        private class Mentorship
        {
            public string DocumentId { get; set; }

            public string Id { get; set; }


        }

        private class MentorshipDocument
        {
            public string DisplayInfoEst { get; set; }

            public string DisplayInfoEng { get; set; }

            public bool IsActive { get; set; }

            public string Degree { get; set; }

            public string DefenceYear { get; set; }
        }

        private class PersonProfileTransformerInfo
        {
            public string IndexId { get; set; }

            public bool IsPublic { get; set; }

            public bool IsConfirmed { get; set; }

            public bool IsConfirmedEng { get; set; }

            public bool HasPublicDataEst { get; set; }

            public bool HasPublicDataEng { get; set; }

            public string PersonId { get; set; }

            public string PersonName { get; set; }

            public IEnumerable<PersonProfileTransformerOccupationInfo> Occupations { get; set; }

            public IEnumerable<PersonProfileTransformerProjectInfo> Projects { get; set; }

            public IEnumerable<PersonProfileTransformerPublicationInfo> Publications { get; set; }

            public IEnumerable<PersonProfileTransformerMentorshipInfo> Mentorships { get; set; }

            public IEnumerable<PersonProfileTransformerMentorshipInfo> MentorshipsFinalized { get; set; }

            public IEnumerable<PersonProfileTransformerMentorshipInfo> MentorshipsDissertation { get; set; }

        }

        private class PersonProfileTransformerPublicationInfo
        {
            public string DocumentId { get; set; }

            public bool IsPublicEst { get; set; }

            public bool IsPublicEng { get; set; }

            public string DisplayString { get; set; }

            public string DisplayStringEng { get; set; }

            public int PublishingYear { get; set; }

            public bool IsActive { get; set; }

            public string ClassificationCode { get; set; }

            public string ClassificationCodeName { get; set; }

            public string ClassificationCodeNameEng { get; set; }

        }

        private class PersonProfileTransformerOccupationInfo
        {
            public string DocumentId { get; set; }

            public string Period { get; set; }

            public bool IsActive { get; set; }

            public bool IsPublicEst { get; set; }

            public bool IsPublicEng { get; set; }

            public string DisplayString { get; set; }

            public string DisplayStringEng { get; set; }


        }

        private class PersonProfileTransformerProjectInfo
        {

            public string DocumentId { get; set; }

            public bool IsActive { get; set; }

            public bool IsPublicEst { get; set; }

            public bool IsPublicEng { get; set; }

            public string DisplayString { get; set; }

            public string DisplayStringEng { get; set; }


            public DateTime EndDate { get; set; }

            public bool PeriodIsActive { get; set; }

        }

        private class PersonProfileTransformerMentorshipInfo
        {
            public string DocumentId { get; set; }

            public bool IsActive { get; set; }

            public bool IsPublicEst { get; set; }

            public bool IsPublicEng { get; set; }

            public string DisplayString { get; set; }

            public string DisplayStringEng { get; set; }

            public string Degree { get; set; }

            public string DefenceYear { get; set; }

        }

        private class ClassifierItemView
        {
            public string Code { get; set; }
        }


    }

}
