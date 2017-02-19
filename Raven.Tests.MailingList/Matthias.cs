using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class Matthias : RavenTest
    {
        public class TestDto
        {
            public string Name { get; set; }
        }

        public class TestDtoContainer
        {
            public TestDtoContainer()
            {
                Dtos = new List<TestDto>();
            }

            public IList<TestDto> Dtos { get; set; }
        }

        public class TransformResult
        {
            public string Name { get; set; }
        }

        public class TransformResult2
        {
            public TransformResult[] Results { get; set; }
        }


        [Fact]
        public void TransformerWorks()
        {
            using (var store = NewDocumentStore())
            {
                new DtoIndex().Execute(store);
                new TestDtoTransformer().Execute(store);
                new TestDtoTransformer2().Execute(store);

                var dto = new TestDto { Name = "Test1" };
                var dto2 = new TestDto { Name = "Test2" };

                var container = new TestDtoContainer
                {
                    Dtos = new[]
                    {
                    new TestDto { Name = "Test1" },
                    new TestDto { Name = "Test2" }
                }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(dto);
                    session.Store(dto2);
                    session.Store(container);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<TestDto, DtoIndex>()
                        .TransformWith<TestDtoTransformer, TransformResult>()
                        .AddTransformerParameter("Name", "Test1")
                        .Where(s => s.Name == "Test1")
                        .ToList();

                    Assert.Equal(2, result.Count);
                    Assert.True(result.All(s => s.Name == "Test1"));
                }
            }
        }

        [Fact]
        public void TransformerFails()
        {
            using (var store = NewRemoteDocumentStore(fiddler:true))
            {
                new DtoIndex().Execute(store);
                new TestDtoTransformer().Execute(store);
                new TestDtoTransformer2().Execute(store);

                var dto = new TestDto { Name = "Test1" };
                var dto2 = new TestDto { Name = "Test2" };

                var container = new TestDtoContainer
                {
                    Dtos = new[]
                    {
                    new TestDto { Name = "Test1" },
                    new TestDto { Name = "Test2" }
                }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(dto);
                    session.Store(dto2);
                    session.Store(container);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {

                    WaitForUserToContinueTheTest(store);
                    var result = session.Query<TestDto, DtoIndex>()
                        .Where(s => s.Name == "Test1")
                        .TransformWith<TestDtoTransformer2, TransformResult2>()
                        .AddTransformerParameter("Name", "Test1")
                        .ToList();

                    Assert.Equal(2, result.Count);
                    Assert.True(result.All(s => s.Results[0].Name == "Test1"));
                }
            }
        }

        public class DtoIndex : AbstractMultiMapIndexCreationTask
        {
            public static readonly string Name = "DtoIndex";

            public override string IndexName => Name;

            public DtoIndex()
            {
                AddMap<TestDtoContainer>(containers =>
                    from container in containers
                    select new
                    {
                        Name = container.Dtos.Select(s => s.Name)
                    });

                AddMap<TestDto>(dtos =>
                    from dto in dtos
                    select new
                    {
                        dto.Name
                    });
            }
        }

        public class TestDtoTransformer : AbstractTransformerCreationTask<object>
        {
            private const string Name = "TestDtoTransformer";

            public TestDtoTransformer()
            {
                TransformResults = results => results
                    .SelectMany(result =>
                        MetadataFor(result).Value<string>("Raven-Entity-Name") == "TestDtos" ?
                            new[] { (TestDto)result } :
                                (MetadataFor(result).Value<string>("Raven-Entity-Name") == "TestDtoContainers" ?
                                    ((TestDtoContainer)result).Dtos :
                                    new TestDto[0]
                                )

                    )
                    .Where(dto => ParameterOrDefault("Name", null).Value<string>() == null || dto.Name == ParameterOrDefault("Name", null).Value<string>())
                    .Select(dto => new { dto.Name });
            }

            public override string TransformerName => Name;
        }

        public class TestDtoTransformer2 : AbstractTransformerCreationTask<object>
        {
            private const string Name = "TestDtoTransformer2";

            public TestDtoTransformer2()
            {
                TransformResults = results =>
                    from result in results
                    let dto =
                    MetadataFor(result).Value<string>("Raven-Entity-Name") == "TestDtos"
                        ? new[] {(TestDto) result}
                        : new object[0]
                    let dtoContainer =
                    MetadataFor(result).Value<string>("Raven-Entity-Name") == "TestDtoContainers"
                        ? ((TestDtoContainer) result).Dtos.ToArray()
                        : new object[0]
                    let dtos = dto.Concat(dtoContainer).Cast<TestDto>()
                    let name = ParameterOrDefault("Name", null).Value<string>()
                    let filteredResult = dtos.Where(s => name == null || s.Name == name)
                    select new
                    {
                        Results = filteredResult
                            .Select(s => new {s.Name})
                    };
            }

            public override string TransformerName => Name;
        }
    }
}
