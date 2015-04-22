using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Core.ChangesApi
{
	public class ImplementingChangesClient : RavenCoreTestBase
	{
		private interface ITypedConnectable : IConnectableChanges<ITypedConnectable>
		{
		}

		private interface IUntypedConnectable : IConnectableChanges
		{
		}

		[Fact]
		public void ClientImplementationShouldWorkWithTypedInterface()
		{
			Task task;

			using (var x = new TypedInterfaceInheritanceChangesClient())
			{
				task = x.Task;
			}

			try
			{
				task.Wait();
			}
			catch (Exception)
			{
			}
		}

		[Fact]
		public void ClientImplementationShouldWorkWithUntypedInterface()
		{
			Task task;

			using (var x = new UntypedInterfaceInheritanceChangesClient())
			{
				task = x.Task;
			}

			try
			{
				task.Wait();
			}
			catch (Exception)
			{
			}
		}

		[Fact]
		public void ShouldFailWhenClientBaseDoesNotImplementConnectableTypeParameterInterface()
		{
			Assert.Throws<InvalidCastException>(() => new NoInterfaceInheritanceChangesClient());
		}

		private class MockConnectionState : IChangesConnectionState
		{
			public Task Task
			{
				get
				{
					throw new NotImplementedException();
				}
			}

			public void Dec()
			{
				throw new NotImplementedException();
			}

			public void Error(Exception e)
			{
				throw new NotImplementedException();
			}

			public void Inc()
			{
				throw new NotImplementedException();
			}
		}

		private class NoInterfaceInheritanceChangesClient : RemoteChangesClientBase<IUntypedConnectable, MockConnectionState>
		{
			public NoInterfaceInheritanceChangesClient()
				: base("http://test", "apiKey", null, new HttpJsonRequestFactory(1024), new DocumentConvention(), () => { })
			{
			}

			protected override void NotifySubscribers(string type, RavenJObject value, IEnumerable<KeyValuePair<string, MockConnectionState>> connections)
			{
				throw new NotImplementedException();
			}

			protected override Task SubscribeOnServer()
			{
				throw new NotImplementedException();
			}
		}

		private class TypedInterfaceInheritanceChangesClient : RemoteChangesClientBase<ITypedConnectable, MockConnectionState>, ITypedConnectable
		{
			public TypedInterfaceInheritanceChangesClient()
				: base("http://test", "apiKey", null, new HttpJsonRequestFactory(1024), new DocumentConvention(), () => { })
			{
			}

			protected override void NotifySubscribers(string type, RavenJObject value, IEnumerable<KeyValuePair<string, MockConnectionState>> connections)
			{
				throw new NotImplementedException();
			}

			protected override Task SubscribeOnServer()
			{
				throw new NotImplementedException();
			}
		}

		private class UntypedInterfaceInheritanceChangesClient : RemoteChangesClientBase<IUntypedConnectable, MockConnectionState>, IUntypedConnectable
		{
			public UntypedInterfaceInheritanceChangesClient()
				: base("http://test", "apiKey", null, new HttpJsonRequestFactory(1024), new DocumentConvention(), () => { })
			{
			}

			protected override void NotifySubscribers(string type, RavenJObject value, IEnumerable<KeyValuePair<string, MockConnectionState>> connections)
			{
				throw new NotImplementedException();
			}

			protected override Task SubscribeOnServer()
			{
				throw new NotImplementedException();
			}
		}
	}
}