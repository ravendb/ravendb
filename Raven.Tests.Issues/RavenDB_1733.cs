// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1733.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
    using Raven.Abstractions.Smuggler;
    using Raven.Json.Linq;
    using Raven.Smuggler.Imports;
    using Xunit;

    public class RavenDB_1733 : NoDisposalNeeded
    {
        private const string emptyTransform = @"function(doc) {
                        return doc;
                    }";

        private readonly SmugglerJintHelper jintHelper;

        public RavenDB_1733()
        {
            jintHelper = new SmugglerJintHelper();
        }

        [Fact]
        public void SmugglerTransformShouldRecognizeNumericPropertiesEvenThoughTheyHaveTheSameNames()
        {
            jintHelper.Initialize(new SmugglerOptions
            {
                TransformScript = emptyTransform
            });

            var testObject = new RavenJObject
			{
				{"Range", new RavenJArray {new RavenJObject {{"Min", 2.4}}}},
				{"Min", 1}
			};

            var transformedObject = jintHelper.Transform(emptyTransform, testObject);

            Assert.Equal(testObject["Min"].Type, transformedObject["Min"].Type);
            Assert.Equal(((RavenJObject)((RavenJArray)testObject["Range"])[0])["Min"].Type, ((RavenJObject)((RavenJArray)transformedObject["Range"])[0])["Min"].Type);

            Assert.True(RavenJToken.DeepEquals(testObject, transformedObject));
        }
    }
}