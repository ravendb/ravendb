//-----------------------------------------------------------------------
// <copyright file="Inheritance.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Document
{
	public class Inheritance : RavenTest
	{
		protected override void ModifyStore(EmbeddableDocumentStore documentStore)
		{
			documentStore.Conventions.FindTypeTagName = type => typeof (IServer).IsAssignableFrom(type) ? "Servers" : null;
		}

		[Fact]
		public void CanStorePolymorphicTypesAsDocuments()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new WindowsServer
					{
						ProductKey = Guid.NewGuid().ToString()
					});
					session.Store(new LinuxServer
					{
						KernelVersion = "2.6.7"
					});
					session.SaveChanges();

                    IServer[] servers = session.Advanced.DocumentQuery<IServer>()
						.WaitForNonStaleResults()
						.ToArray();
					Assert.Equal(2, servers.Length);
				}
			}
		}

		public class WindowsServer  : IServer
		{
			public string Id { get; set; }
			public string ProductKey { get; set; }

			public void Start()
			{
				
			}
		}

		public class LinuxServer : IServer
		{
			public string Id { get; set; }
			public string KernelVersion { get; set; }
			
			public void Start()
			{
				
			}
		}

		public interface IServer
		{
			void Start();
		}
	}

	
}
