using System;
using System.Linq;
using System.Runtime.Serialization;
using FastTests;
using Tests.Infrastructure;
using Xunit;

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
            Assert.Equal(GetDescription(MyEnum.First), GetDescription(result.First().MyEnum));
        }

        record MyClass(MyEnum MyEnum);

        enum MyEnum
        {
            [EnumMember(Value = "first_my_name")]
            First,

            [EnumMember(Value = "second_my_name")]
            Second
        }
        
        private static string GetDescription(Enum value)
        {
            var fi = value.GetType().GetField(value.ToString());

            if (fi != null)
            {
                var attributes = (EnumMemberAttribute[])fi.GetCustomAttributes(typeof(EnumMemberAttribute), false);
                return (attributes.Length > 0) ? attributes[0].Value : value.ToString();
            }

            return value.ToString();
        }
    }
}
