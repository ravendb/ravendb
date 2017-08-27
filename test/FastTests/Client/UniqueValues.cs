using Raven.Client.Exceptions.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client
{
    public class UniqueValues : RavenTestBase
    {
        [Fact]
        public void CanPutUniqueString()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var putCmd = new CompareExchangeOperation<string>("test", "Karmel", 0);
            var serverOperationExecutor = new ServerOperationExecutor(store);
            serverOperationExecutor.Send(putCmd);
            var getCmd = new GetClusterValue<string>("test");
            var res = serverOperationExecutor.Send(getCmd);
            Assert.Equal("Karmel", res.Value);
        }

        [Fact]
        public void CanPutUniqueObject()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var putCmd = new CompareExchangeOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0);
            var serverOperationExecutor = new ServerOperationExecutor(store);
            serverOperationExecutor.Send(putCmd);
            var getCmd = new GetClusterValue<User>("test");
            var res = serverOperationExecutor.Send(getCmd);
            Assert.Equal("Karmel", res.Value.Name);
        }

        [Fact]
        public void CanPutMultiDifferentValues()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var putCmd = new CompareExchangeOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0);
            var putCmd2 = new CompareExchangeOperation<User>("test2", new User
            {
                Name = "Karmel"
            }, 0);
            var serverOperationExecutor = new ServerOperationExecutor(store);
            var serverOperationExecutor2 = new ServerOperationExecutor(store);
            serverOperationExecutor.Send(putCmd);
            serverOperationExecutor2.Send(putCmd2);

            var getCmd = new GetClusterValue<User>("test");
            var getCmd2 = new GetClusterValue<User>("test2");
            var res = serverOperationExecutor.Send(getCmd);
            var res2 = serverOperationExecutor.Send(getCmd2);
            Assert.Equal("Karmel", res.Value.Name);
            Assert.Equal("Karmel", res2.Value.Name);
        }

        [Fact]
        public void ThrowWhenPuttingConcurrently()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var putCmd = new CompareExchangeOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0);
            var putCmd2 = new CompareExchangeOperation<User>("test", new User
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
            var getCmd = new GetClusterValue<User>("test");
            var res = serverOperationExecutor.Send(getCmd);
            Assert.Equal("Karmel", res.Value.Name);
        }
    }
}
