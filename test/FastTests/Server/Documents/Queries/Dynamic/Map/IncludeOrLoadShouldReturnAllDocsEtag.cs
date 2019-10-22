using Raven.Server.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Queries.Dynamic.Map
{
    public class IncludeOrLoadShouldReturnAllDocsEtag : NoDisposalNeeded
    {
        public IncludeOrLoadShouldReturnAllDocsEtag(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void FunctionWithLoad()
        {
            var query = new IndexQueryServerSide(@"
declare function getCompanyName(c) {
    return load(c.Company).Name;
}

from Orders as o
where id() == 'orders/1'
select {
    Company: o.Company,
    Name: Compute(o)
}");

            Assert.True(query.Metadata.HasIncludeOrLoad);
        }

        [Fact]
        public void SelectWithLoad()
        {
            var query = new IndexQueryServerSide(@"
from Orders as o
where id() == 'orders/1'
select {
    Company: o.Company,
    Name: load(o.Company).Name
}");

            Assert.True(query.Metadata.HasIncludeOrLoad);
        }

        [Fact]
        public void Load()
        {
            var query = new IndexQueryServerSide(@"
from Orders as o
where id() == 'orders/1'
load o.Company as c 
select c.Name
");

            Assert.True(query.Metadata.HasIncludeOrLoad);
        }

        [Fact]
        public void Include()
        {
            var query = new IndexQueryServerSide(@"
from Orders as o
where id() == 'orders/1'
select o.Company
include o.Company
");

            Assert.True(query.Metadata.HasIncludeOrLoad);
        }

        [Fact]
        public void WithoutIncludeOrLoad()
        {
            var query = new IndexQueryServerSide(@"
from Orders as o
where id() == 'orders/1'
select {
    Company: o.Company
}");

            Assert.False(query.Metadata.HasIncludeOrLoad);
        }
    }
}
