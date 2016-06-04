using System.Linq;

using Raven.Abstractions.Indexing;

using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3899 : RavenTest
    {
        [Fact]
        public void CanSaveTransformerWithMultipleSelectMany()
        {
            using (var store = NewRemoteDocumentStore())
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
                store.DatabaseCommands.PutTransformer("T1", t1);
            }
        }

        [Fact]
        public void CanSaveTransformerWithCastToDynamic()
        {
            using (var store = NewRemoteDocumentStore())
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
                store.DatabaseCommands.PutTransformer("T1", t1);
            }
        }
    }
}
