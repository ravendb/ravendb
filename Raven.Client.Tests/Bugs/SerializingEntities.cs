using System;
using System.ComponentModel;
using System.IO;
using Newtonsoft.Json;
using Raven.Client.Document;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class SerializingEntities
	{
		public class Foo : INotifyPropertyChanged
		{
			public string Id { get; set; }
			public event PropertyChangedEventHandler PropertyChanged;

			public void InvokePropertyChanged(PropertyChangedEventArgs e)
			{
				PropertyChangedEventHandler handler = PropertyChanged;
				if (handler != null) handler(this, e);
			}
		}

		public class Bar
		{
			public string NotSerializable
			{
				get
				{
					throw new Exception("This shouldn't be serialized");
				}
			}
			public void FooChanged(object sender, PropertyChangedEventArgs e)
			{
			}
		}

		[Fact]
		public void WillNotSerializeEvents()
		{
			if (Directory.Exists("Data")) 
				Directory.Delete("Data", true);
			try
			{
				using (var documentStore = new DocumentStore())
				{
					documentStore.Configuration.DataDirectory = "Data";
					documentStore.Conventions.CustomizeJsonSerializer = x => x.TypeNameHandling = TypeNameHandling.Auto;
					documentStore.Initialize();

					var bar = new Bar();
					var foo = new Foo();
					foo.PropertyChanged += bar.FooChanged;

					using (var session = documentStore.OpenSession())
					{
						session.Store(foo);
						session.SaveChanges();
					}
				}
			}
			finally
			{
				if (Directory.Exists("Data"))
					Directory.Delete("Data", true);
			}
		}
	}
}