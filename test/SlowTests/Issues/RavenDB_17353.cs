using System;
using FastTests;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17353 : RavenTestBase
    {
        public RavenDB_17353(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CheckGetNodeByTag()
        {
            const string dbid1 = "07e2GrSMdkunq1AC+KgwIg";
            const string dbid2 = "F9I6Egqwm0Kz+K0oFVIR9Q";
            const string cv = "C:8397-07e2GrSMdkunq1AC+KgwIg, A:8917-3UiZOcXaZ0+d6GI/VTr//A, B:8397-5FYpkl5TX0SPlIBPwjmhUw, A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ";
            
            var nodeTag = ChangeVectorUtils.GetNodeTagById(cv, dbid1);
            Assert.Equal("C", nodeTag);

            nodeTag = ChangeVectorUtils.GetNodeTagById(cv, dbid2);
            Assert.NotEqual(" A", nodeTag); 
            Assert.Equal("A", nodeTag); 
        }
    }
}
