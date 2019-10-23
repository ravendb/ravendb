using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class CanDeployIndex : RavenTestBase
    {
        public CanDeployIndex(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void can_deploy_index1()
        {
            using (var store = GetDocumentStore())
            {
                new DocIndex1().Execute(store);
            }
        }

        [Fact]
        public void can_deploy_index2()
        {
            using (var store = GetDocumentStore())
            {
                new DocIndex2().Execute(store);
            }
        }

    }

    public class DocIndex1 : AbstractIndexCreationTask<Doc>
    {
        public DocIndex1()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              Id = doc.Id,
                              DoubleValue = !double.IsNaN((double)(doc.DoubleValue)) && !double.IsInfinity((double)(doc.DoubleValue))
                                  ? doc.DoubleValue
                                  : (double?)null,
                          };
        }
    }

    public class DocIndex2 : AbstractIndexCreationTask<doc>
    {
        public DocIndex2()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              Id = doc.Id,
                              DoubleValue = !double.IsNaN((double)(doc.DoubleValue)) && !double.IsInfinity((double)(doc.DoubleValue))
                                  ? doc.DoubleValue
                                  : (double?)null,
                          };
        }
    }

    public class Doc
    {
        public string Id { get; set; }
        public double DoubleValue { get; set; }
    }

    public class doc : Doc
    {

    }
}
