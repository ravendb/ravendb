using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16041 : RavenTestBase
    {
        public RavenDB_16041(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
        }

        public class MyIndex : AbstractJavaScriptIndexCreationTask
        {
            public MyIndex()
            {
                Maps = new HashSet<string>
                {
                    @"map('Items', (d) => {
                    {
                    return [
                    {
                    a: {
                    b: 1,
                    c: 2
                },
                d: 3
                },
                {
                    a: {
                        b: 1,
                        c: 2
                    },
                    d: 4
                },
                {
                    a: {
                        b: 1,
                        c: 3
                    },
                    d: 5
                },
                {
                    a: {
                        b: 2,
                        c: 2
                    },
                    d: 6
                }
                ]
                }
                })"
                };
                Reduce = @"groupBy(x => ({ a: x.a }))
.aggregate(g => { 
    return {
        a: Object.assign({}, g.key.a),
        d: g.values.reduce((res, val) => res + val.d, 0)
    };
})

// Jint doesn't support it in our version, taken
// from: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/assign

if (typeof Object.assign !== 'function') {
  // Must be writable: true, enumerable: false, configurable: true
  Object.defineProperty(Object, 'assign', {
    value: function assign(target, varArgs) { // .length of function is 2
      'use strict';
      if (target === null || target === undefined) {
        throw new TypeError('Cannot convert undefined or null to object');
      }

      var to = Object(target);

      for (var index = 1; index < arguments.length; index++) {
        var nextSource = arguments[index];

        if (nextSource !== null && nextSource !== undefined) {
          for (var nextKey in nextSource) {
            // Avoid bugs when hasOwnProperty is shadowed
            if (Object.prototype.hasOwnProperty.call(nextSource, nextKey)) {
              to[nextKey] = nextSource[nextKey];
            }
          }
        }
      }
      return to;
    },
    writable: true,
    configurable: true
  });
}
";
            }
        }

        [Fact]
        public void CanUseComplexObjectInJavascriptMapReduce()
        {
            using var store = GetDocumentStore();
            using (var s = store.OpenSession())
            {
                s.Store(new Item());
                s.SaveChanges();
            }
            new MyIndex().Execute(store);
            WaitForIndexing(store, allowErrors: false);

            using (var s = store.OpenSession())
            {
                Assert.NotEmpty(s.Query<object, MyIndex>().ToList());
                Assert.Equal(3, s.Query<object, MyIndex>().Count());
            }
        }
    }
}
