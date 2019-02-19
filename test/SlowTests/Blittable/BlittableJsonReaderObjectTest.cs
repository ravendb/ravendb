using FastTests;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Blittable
{
    public class BlittableJsonReaderObjectTest : RavenTestBase
    {
        [Fact]
        public void Clone_WhenContainItemsOfStrings_ShouldBeEqualToOrigin()
        {
            //Arrange
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var data = new { Property = new[] { "Value1", "Value2" } };
                var readerObject = EntityToBlittable.ConvertCommandToBlittable(data, context);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(context);
                    
                //Assert
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void Clone_WhenContainItemsOfIntegers_ShouldBeEqualToOrigin()
        {
            //Arrange
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var data = new { Property = new[] { 1, 2 } };
                var readerObject = EntityToBlittable.ConvertCommandToBlittable(data, context);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(context);
                
                //Assert
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void Clone_WhenContainItemsOfDouble_ShouldBeEqualToOrigin()
        {
            //Arrange
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var data = new { Property = new[] { 1.1, 2.3 } };
                var readerObject = EntityToBlittable.ConvertCommandToBlittable(data, context);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(context);

                //Assert
                Assert.Equal(expected, actual);
            }
        }

        //Todo Fixing the clone implementation to support this situation or throw clear error
        [Fact(Skip = "Should fixing the clone implementation to support this situation or throw clear error")]
        public void Clone_WhenContainItemsOfObjects_AndOriginAndCloneOnSameContext_ShouldBeEqualToOrigin()
        {
            //Arrange
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var data = new
                {
                    Property = new[] { new { Prop = "Value1" }, new { Prop = "Value2" } }
                };
                var readerObject = EntityToBlittable.ConvertCommandToBlittable(data, context);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(context);

                //Assert
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
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
                var readerObject = EntityToBlittable.ConvertCommandToBlittable(data, originContext);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(cloneContext);

                //Assert
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
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
                var readerObject = EntityToBlittable.ConvertCommandToBlittable(data, originContext);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                //Action
                var actual = expected.Clone(cloneContext);

                //Assert
                Assert.Equal(expected, actual);
            }
        }
    }
}
