// -----------------------------------------------------------------------
//  <copyright file="RDBQA_17.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Abstractions.Indexing;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBQA_17 : RavenTestBase
    {
        [Fact]
        public void WhenOverridingTransformerOldOneShouldBeDeleted()
        {
            const string Name = "users/selectName";

            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutTransformer(Name, new TransformerDefinition
                {
                    Name = Name,
                    TransformResults = "from user in results select new { user.Age, user.Name }"
                });

                var transformers = store.DatabaseCommands.GetTransformers(0, 10);
                var transformerId = transformers[0].TransfomerId;

                store.DatabaseCommands.PutTransformer(Name, new TransformerDefinition
                {
                    Name = Name,
                    TransformResults = "from user in results select new { Name = user.Name }"
                });

                transformers = store.DatabaseCommands.GetTransformers(0, 10);

                Assert.Equal(1, transformers.Length);
                Assert.Equal("from user in results select new { Name = user.Name }", transformers[0].TransformResults);
                Assert.NotEqual(transformerId, transformers[0].TransfomerId);
            }
        }
    }
}
