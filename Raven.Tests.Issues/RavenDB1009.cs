using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB1009 : RavenTest
    {
        class Foo
        {
            public byte[] Hash { get; set; }
        }

        [Fact]
        public void CanHandleWhenSettingByteArrayToNull()
        {
            using (var store = NewDocumentStore())
            {
                // store a doc
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo(), "foos/1");
                    session.SaveChanges();
                }

                // store a doc
                using (var session = store.OpenSession())
                {
                    var foo = session.Load<Foo>("foos/1");
                    foo.Hash = new byte[100];
                    session.SaveChanges();
                }
            }
        }
    }
}
