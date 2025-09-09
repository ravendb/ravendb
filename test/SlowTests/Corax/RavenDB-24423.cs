using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_24423 : RavenTestBase
{
    public RavenDB_24423(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanSortBengaliAlphabetByOrderByAlphaNumeric(Options options)
    {
        using var store = GetDocumentStore(options);
        var localDtos = new List<Dto>();

        using (var session = store.OpenSession())
        {
            var titles = new List<string>
            {
                "বাংলাবর্ণমালাবালিপি",
                "বাংলাবর্ণমালা1"
            };

            titles.ForEach(x => localDtos.Add(new Dto(x)));

            foreach (var dto in localDtos)
            {
                session.Store(dto);
            }

            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var dtosFromServer = session.Query<Dto>()
                .OrderBy(x => x.Title, OrderingType.AlphaNumeric)
                .Select(x => x.Title)
                .ToList();

            Assert.Equal(2, dtosFromServer.Count);

            localDtos.Sort(new AlphaNumericDtoOrder(titleDescending: false));

            Assert.Equal(localDtos.Select(x => x.Title), dtosFromServer);
        }

        using (var session = store.OpenSession())
        {
            var dtosFromServer = session.Query<Dto>()
                .OrderByDescending(x => x.Title, OrderingType.AlphaNumeric)
                .Select(x => x.Title)
                .ToList();

            Assert.Equal(2, dtosFromServer.Count);

            localDtos.Sort(new AlphaNumericDtoOrder(titleDescending: true));

            Assert.Equal(localDtos.Select(x => x.Title), dtosFromServer);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanSortBengaliNumericByOrderByAlphaNumeric(Options options)
    {
        using var store = GetDocumentStore(options);
        var localDtos = new List<Dto>();

        using (var session = store.OpenSession())
        {
            var titles = new List<string>
            {
                "১০০",
                "১১০"
            };

            titles.ForEach(x => localDtos.Add(new Dto(x)));

            foreach (var dto in localDtos)
            {
                session.Store(dto);
            }

            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var dtosFromServer = session.Query<Dto>()
                .OrderBy(x => x.Title, OrderingType.AlphaNumeric)
                .Select(x => x.Title)
                .ToList();

            Assert.Equal(2, dtosFromServer.Count);

            localDtos.Sort(new AlphaNumericDtoOrder(titleDescending: false));

            Assert.Equal(localDtos.Select(x => x.Title), dtosFromServer);
        }

        using (var session = store.OpenSession())
        {
            var dtosFromServer = session.Query<Dto>()
                .OrderByDescending(x => x.Title, OrderingType.AlphaNumeric)
                .Select(x => x.Title)
                .ToList();

            Assert.Equal(2, dtosFromServer.Count);

            localDtos.Sort(new AlphaNumericDtoOrder(titleDescending: true));

            Assert.Equal(localDtos.Select(x => x.Title), dtosFromServer);
        }
    }

    private class AlphaNumericDtoOrder : IComparer<Dto>
    {
        private readonly bool titleDescending;

        public AlphaNumericDtoOrder(bool titleDescending = false)
        {
            this.titleDescending = titleDescending;
        }

        public int Compare(Dto dto1, Dto dto2)
        {
            if (dto1.Title == null && dto2.Title != null)
                return -1;
            else if (dto1.Title != null && dto2.Title == null)
                return 1;

            if (dto1.Title == null && dto2.Title == null)
                return 0;

            int result = titleDescending == false ?
                AlphaNumericFieldComparator.UnmanagedStringAlphanumComparer.Instance.Compare(dto1.Title, dto2.Title) :
                AlphaNumericFieldComparator.UnmanagedStringAlphanumComparer.Instance.Compare(dto2.Title, dto1.Title);

            return result;
        }
    }

    private record Dto(string Title, string Id = null);
}
