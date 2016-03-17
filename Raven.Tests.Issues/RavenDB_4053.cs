using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4053 : RavenTestBase
    {
        [Fact]
        public void Index_with_custom_class_array()
        {
            try
            {
                using (var store = NewDocumentStore())
                {
                    store.ExecuteIndex(new ArrayIndex());
                }
            }
            catch (IndexCompilationException e)
            {
                Assert.True(false,"Index failed to compile on server.");
            }
        }
    }

    public class IndexedDoc
    {

    }

    public class SomeClass
    {

    }

    public class ArrayIndex : AbstractIndexCreationTask<IndexedDoc>
    {
        public ArrayIndex()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              Array = new SomeClass[0]
                          };
        }
    }
}
