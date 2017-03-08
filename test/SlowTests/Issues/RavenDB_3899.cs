using FastTests;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3899 : RavenTestBase
    {
        [Fact]
        public void CanSaveTransformerWithMultipleSelectMany()
        {
            using (var store = GetDocumentStore())
            {
                var t1 = new TransformerDefinition
                {
                    Name = "T1",
                    TransformResults = "from people in results " +
                         " from child in people.Children " +
                         " from grandchild in child.Children " +
                         " from great in grandchild.Children " +
                         " select new " +
                         "  { " +
                         "     Name = child.Name  " +
                         "  }"
                };
                store.Admin.Send(new PutTransformerOperation(t1));
            }
        }

        [Fact]
        public void CanSaveTransformerWithCastToDynamic()
        {
            using (var store = GetDocumentStore())
            {
                var t1 = new TransformerDefinition
                {
                    Name = "T1",
                    TransformResults = "from people in results " +
                         " from child in (IEnumerable<dynamic>)people.Children " +
                         " from grandchild in (IEnumerable<dynamic>)child.Children " +
                         " from great in (IEnumerable<dynamic>)grandchild.Children " +
                         " select new " +
                         "  { " +
                         "     Name = child.Name  " +
                         "  }"
                };
                store.Admin.Send(new PutTransformerOperation(t1));
            }
        }
    }
}
