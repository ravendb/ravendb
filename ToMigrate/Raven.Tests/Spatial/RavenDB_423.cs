// -----------------------------------------------------------------------
//  <copyright file="RavenDB_423.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Spatial
{
    public class RavenDB_423 : RavenTest
    {
        [Fact]
        public void WillThrowOnSpatialGenerateInTransformResults()
        {
            using(var store = NewDocumentStore())
            {
                Assert.Throws<TransformCompilationException>(() => store.DatabaseCommands.PutTransformer("test", new TransformerDefinition()
                {
                    Name = "test",
                    TransformResults = "from result in results select new { _= SpatialIndex.Generate(result.x, result.Y)}"
                }));
            }
        }

        [Fact]
        public void WillThrowOnCreateFieldInTransformResults()
        {
            using (var store = NewDocumentStore())
            {
                Assert.Throws<TransformCompilationException>(() => store.DatabaseCommands.PutTransformer("test", new TransformerDefinition()
                {
                    Name = "test",
                    TransformResults = "from result in results select new { _= CreateField(result.x, result.Y)}"
                }));
            }
        }
    }
}
