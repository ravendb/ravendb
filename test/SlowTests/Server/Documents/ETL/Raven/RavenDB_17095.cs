using System;
using Xunit;
using Xunit.Abstractions;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations.ETL;
using Tests.Infrastructure;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_17095 : RavenTestBase
    {
        public RavenDB_17095(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenFact(RavenTestCategory.Etl)]
        public void Should_throw_when_transforms_is_empty()
        {
            var configuration = new RavenEtlConfiguration {ConnectionStringName = "test", Name = "myConfig", MentorNode = "A", Transforms = new List<Transformation>()};
            configuration.Initialize(new RavenConnectionString());
            var e = Assert.Throws<InvalidOperationException>(() => configuration.Validate(out List<string> _));
            Assert.Equal($"'{nameof(RavenEtlConfiguration.Transforms)}' list cannot be empty.",e.Message);
        }

        [RavenFact(RavenTestCategory.Etl)]
        public void Should_pass_when_transforms_is_not_empty()
        {
            var configuration = new RavenEtlConfiguration
            {
                ConnectionStringName = "test", Name = "myConfig", MentorNode = "A", Transforms = new List<Transformation> {new()}
            };
            configuration.Initialize(new RavenConnectionString());
            configuration.Validate(out List<string> _);
        }
    }
}
