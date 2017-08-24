using Raven.Client.Exceptions.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client
{
    class UniqueValues : RavenTestBase
    {
        [Fact]
        public void CanPutUniqueString()
        {
            var store = GetDocumentStore();
            var putCmd = new UniqueValueOperation<string>("test", "Karmel", 0);
            var serverOperationExecutor = new ServerOperationExecutor(store);
            serverOperationExecutor.Send(putCmd);
            var getCmd = new GetUniqueValue<string>("test");
            var res = serverOperationExecutor.Send(getCmd);
            Assert.Equal("Karmel", res.Value);
        }

        [Fact]
        public void CanPutUniqueObject()
        {
            var store = GetDocumentStore();
            var putCmd = new UniqueValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0);
            var serverOperationExecutor = new ServerOperationExecutor(store);
            serverOperationExecutor.Send(putCmd);
            var getCmd = new GetUniqueValue<User>("test");
            var res = serverOperationExecutor.Send(getCmd);
            Assert.Equal("Karmel", res.Value.Name);
        }

        [Fact]
        public void CanPutMultiDifferentValues()
        {
            var store = GetDocumentStore();
            var putCmd = new UniqueValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0);
            var putCmd2 = new UniqueValueOperation<User>("test2", new User
            {
                Name = "Karmel"
            }, 0);
            var serverOperationExecutor = new ServerOperationExecutor(store);
            var serverOperationExecutor2 = new ServerOperationExecutor(store);
            serverOperationExecutor.Send(putCmd);
            serverOperationExecutor2.Send(putCmd2);

            var getCmd = new GetUniqueValue<User>("test");
            var getCmd2 = new GetUniqueValue<User>("test2");
            var res = serverOperationExecutor.Send(getCmd);
            var res2 = serverOperationExecutor.Send(getCmd2);
            Assert.Equal("Karmel", res.Value.Name);
            Assert.Equal("Karmel", res2.Value.Name);
        }

        [Fact]
        public void ThrowWhenPuttingConcurrently()
        {
            var store = GetDocumentStore();
            var putCmd = new UniqueValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0);
            var putCmd2 = new UniqueValueOperation<User>("test", new User
            {
                Name = "Karmel2"
            }, 0);
            var serverOperationExecutor = new ServerOperationExecutor(store);
            var serverOperationExecutor2 = new ServerOperationExecutor(store);
            serverOperationExecutor.Send(putCmd);
            Assert.Throws<CommandExecutionException>(() =>
            {
                serverOperationExecutor2.Send(putCmd2);
            });
            var getCmd = new GetUniqueValue<User>("test");
            var res = serverOperationExecutor.Send(getCmd);
            Assert.Equal("Karmel", res.Value.Name);
        }
    }
}
