using System.Linq;
using System.Runtime.Serialization;
using Elastic.Transport.Extensions;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23935 : RavenTestBase
    { 
        public RavenDB_23935(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void EnumMemberAttributeTest()
        {
            using var store = GetDocumentStore();
            using var session = store.OpenSession();
            var str = new MyClass(MyEnum.First);
            session.Store(new MyClass(MyEnum.First));
            session.SaveChanges();

            var result = session.Query<MyClass>()
                .Customize(p => p.WaitForNonStaleResults())
                .Where(x => x.MyEnum == MyEnum.First)
                .ToList();

            Assert.Single(result);
            Assert.Equal(MyEnum.First.GetStringValue(), result.First().MyEnum.GetStringValue());
        }

        record MyClass(MyEnum MyEnum);

        enum MyEnum
        {
            [EnumMember(Value = "first_my_name")]
            First,

            [EnumMember(Value = "second_my_name")]
            Second
        }
    }
}
