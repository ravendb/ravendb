using System.Threading;
using Raven.Abstractions.Connection;
using Raven.Client;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.ChangesApi
{
    public class ImplementingChangesClient : RavenCoreTestBase
    {
#if DNXCORE50
        public ImplementingChangesClient(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        private interface IUntypedConnectable : IConnectableChanges
        { }

        private class NoInterfaceInheritanceChangesClient : RemoteChangesClientBase<IUntypedConnectable, MockConnectionState>
        {
            public NoInterfaceInheritanceChangesClient() :
                base("http://test", "apiKey", null, new HttpJsonRequestFactory(1024), new DocumentConvention(), new MockReplicationInformerBase(), () => { })
            {
            }

            protected override Task SubscribeOnServer()
            {
                throw new NotImplementedException();
            }

            protected override void NotifySubscribers(string type, RavenJObject value, List<MockConnectionState> connections)
            {
                throw new NotImplementedException();
            }
        }

        [Fact]
        public void ShouldFailWhenClientBaseDoesNotImplementConnectableTypeParameterInterface()
        {
            Assert.Throws<InvalidCastException>(() => new NoInterfaceInheritanceChangesClient());
        }



        private class UntypedInterfaceInheritanceChangesClient : RemoteChangesClientBase<IUntypedConnectable, MockConnectionState>, IUntypedConnectable
        {
            public UntypedInterfaceInheritanceChangesClient() :
                base("http://test", "apiKey", null, new HttpJsonRequestFactory(1024), new DocumentConvention(), new MockReplicationInformerBase(), () => { })
            {
            }

            protected override Task SubscribeOnServer()
            {
                throw new NotImplementedException();
            }

            protected override void NotifySubscribers(string type, RavenJObject value, List<MockConnectionState> connections)
            {
                throw new NotImplementedException();
            }
        }

        [Fact]
        public void ClientImplementationShouldWorkWithUntypedInterface()
        {
            using (var untypedInterfaceInheritanceChangesClient = new UntypedInterfaceInheritanceChangesClient())
            {
                try
                {
                    untypedInterfaceInheritanceChangesClient.Task.Wait();
                }
                catch (Exception)
                {
                    
                }
            }
        }


        private interface ITypedConnectable : IConnectableChanges<ITypedConnectable>
        {
        }

        class TypedInterfaceInheritanceChangesClient : RemoteChangesClientBase<ITypedConnectable, MockConnectionState>, ITypedConnectable
        {
            public TypedInterfaceInheritanceChangesClient() :
                base("http://test", "apiKey", null, new HttpJsonRequestFactory(1024), new DocumentConvention(), new MockReplicationInformerBase(), () => { })
            {
            }

            protected override Task SubscribeOnServer()
            {
                throw new NotImplementedException();
            }

            protected override void NotifySubscribers(string type, RavenJObject value, List<MockConnectionState> connections)
            {
                throw new NotImplementedException();
            }

            public new Task<ITypedConnectable> Task
            {
                get { throw new NotImplementedException(); }
            }
        }

        [Fact]
        public void ClientImplementationShouldWorkWithTypedInterface()
        {
            new TypedInterfaceInheritanceChangesClient();
        }


        #region Mocks

        private class MockConnectionState : IChangesConnectionState
        {
            public Task Task
            {
                get { throw new NotImplementedException(); }
            }

            public void Inc()
            {
                throw new NotImplementedException();
            }

            public void Dec()
            {
                throw new NotImplementedException();
            }

            public void Error(Exception e)
            {
                throw new NotImplementedException();
            }
        }

        private class MockReplicationInformerBase : IReplicationInformerBase
        {
#pragma warning disable 67
            public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged;
#pragma warning restore 67

            public int DelayTimeInMiliSec
            {
                get
                {
                    throw new NotImplementedException();
                }
                set
                {
                    throw new NotImplementedException();
                }
            }

            public List<OperationMetadata> ReplicationDestinations
            {
                get { throw new NotImplementedException(); }
            }

            public List<OperationMetadata> ReplicationDestinationsUrls
            {
                get { throw new NotImplementedException(); }
            }

            public long GetFailureCount(string operationUrl)
            {
                throw new NotImplementedException();
            }

            public DateTime GetFailureLastCheck(string operationUrl)
            {
                throw new NotImplementedException();
            }

            public int GetReadStripingBase(bool increment)
            {
                throw new NotImplementedException();
            }

            public Task<T> ExecuteWithReplicationAsync<T>(string method, string primaryUrl, OperationCredentials primaryCredentials, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, Task<T>> operation, CancellationToken token)
            {
                throw new NotImplementedException();
            }

            public void ForceCheck(string primaryUrl, bool shouldForceCheck)
            {
                throw new NotImplementedException();
            }

            public bool IsServerDown(Exception exception, out bool timeout)
            {
                throw new NotImplementedException();
            }

            public bool IsHttpStatus(Exception e, out HttpStatusCode statusCode, params HttpStatusCode[] httpStatusCode)
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }
        }

        #endregion Mocks
    }
}
