using FastTests;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Blittable
{
    public class BlittableJsonReaderObjectTest : RavenTestBase
    {
        public BlittableJsonReaderObjectTest(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Clone_WhenContainItemsOfStrings_ShouldBeEqualToOrigin()
        {
            //Arrange
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var data = new { Property = new[] { "Value1", "Value2" } };
                var readerObject = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(context);
                    
                //Assert
                Assert.Equal(expected, actual);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Clone_WhenContainItemsOfIntegers_ShouldBeEqualToOrigin()
        {
            //Arrange
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var data = new { Property = new[] { 1, 2 } };
                var readerObject = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(context);
                
                //Assert
                Assert.Equal(expected, actual);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Clone_WhenContainItemsOfDouble_ShouldBeEqualToOrigin()
        {
            //Arrange
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var data = new { Property = new[] { 1.1, 2.3 } };
                var readerObject = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(context);

                //Assert
                Assert.Equal(expected, actual);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Clone_WhenContainItemsOfObjects_AndOriginAndCloneOnSameContext_ShouldBeEqualToOrigin()
        {
            //Arrange
            using var context = JsonOperationContext.ShortTermSingleUse();
            var data = new
            {
                Property = new object[] { new { Prop1 = "Value1" }, new { Prop2 = "Value2" } }
            };
            var readerObject = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
            readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

            //Action
            var actual = expected.Clone(context);

            //Assert
            Assert.Equal(expected, actual);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Clone_WhenContainItemsOfObjects_AndOriginAndCloneOnDifferentContext_ShouldBeEqualToOrigin()
        {
            //Arrange
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext originContext))
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext cloneContext))
            {
                var data = new
                {
                    Property = new[] { new { Prop = "Value1" }, new { Prop = "Value2" } }
                };
                var readerObject = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, originContext);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(cloneContext);

                //Assert
                Assert.Equal(expected, actual);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Clone_WhenContainMixItemTypes_AndOriginAndCloneOnDifferentContext_ShouldBeEqualToOrigin()
        {
            //Arrange
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext originContext))
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext cloneContext))
            {
                var data = new
                {
                    Property = new object[] { new { Prop = "Value1" }, "Value2", 4 }
                };
                var readerObject = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, originContext);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(cloneContext);

                //Assert
                Assert.Equal(expected, actual);
            }
        }
    }
}
