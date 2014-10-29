using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class MultiGetDoesNotUrlDecodeTwice : RavenTest
    {
        [Fact]
        public void ShouldNotDecodeTwice()
        {
            /* description:
             * Older clients of RavenDb used to double encode the URL query string contents.
             * This is not the case anymore, and double decoding the query query string may have side effects 
             * (e.g.: the '+' character will get decoded into a whitespace ' ' character
            */
            using (var store = NewRemoteDocumentStore())
            {
                const string someSpecialString = "+";

                using (var session = store.OpenSession())
                {
                    var instance = new SomeEntity { SomeText = someSpecialString };

                    session.Store(instance);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var lazyResults = session.Query<SomeEntity>()
                              .Customize(c => c.WaitForNonStaleResults())
                              .Where(x => x.SomeText == someSpecialString)
                              .Lazily();


                    var actualResults = lazyResults.Value.ToArray();

                    Assert.NotEmpty(actualResults);
                }
            }
        }

        public class SomeEntity
        {
            public string SomeText { get; set; }
        }
    }
}
