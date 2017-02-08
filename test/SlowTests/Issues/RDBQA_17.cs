// -----------------------------------------------------------------------
//  <copyright file="RDBQA_17.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Operations.Databases.Transformers;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBQA_17 : RavenNewTestBase
    {
        [Fact]
        public void WhenOverridingTransformerOldOneShouldBeDeleted()
        {
            const string Name = "users/selectName";

            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutTransformerOperation(Name, new TransformerDefinition
                {
                    Name = Name,
                    TransformResults = "from user in results select new { user.Age, user.Name }"
                }));

                var transformers = store.Admin.Send(new GetTransformersOperation(0, 10));
                var transformerId = transformers[0].TransfomerId;

                store.Admin.Send(new PutTransformerOperation(Name, new TransformerDefinition
                {
                    Name = Name,
                    TransformResults = "from user in results select new { Name = user.Name }"
                }));

                transformers = store.Admin.Send(new GetTransformersOperation(0, 10));

                Assert.Equal(1, transformers.Length);
                Assert.Equal("from user in results select new { Name = user.Name }", transformers[0].TransformResults);
                Assert.NotEqual(transformerId, transformers[0].TransfomerId);
            }
        }
    }
}
