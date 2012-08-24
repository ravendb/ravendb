
using System.Reflection;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Client.Document;

namespace Raven.Tests.Json
{
	using System.Collections.Generic;
	using Xunit;

	public class JsonNetBugsTests : RavenTest
	{
		class ObjectyWithByteArray
		{
			public byte[] Data { get; set; }
		}

		[Fact]
		public void can_serialize_object_whth_byte_array_when_TypeNameHandling_is_All()
		{
			ObjectyWithByteArray data = new ObjectyWithByteArray { Data = new byte[] { 72, 63, 62, 71, 92, 55 } };
			using (var store = NewDocumentStore())
			{
				// this is an edge case since it does not make a lot of sense for users to set this.
				store.Conventions.CustomizeJsonSerializer = x => x.TypeNameHandling = Raven.Imports.Newtonsoft.Json.TypeNameHandling.All;

				using (var session = store.OpenSession())
				{
					session.Store(data, "test");
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					var result = session.Load<ObjectyWithByteArray>("test");
					Assert.NotNull(result);
					Assert.Equal(data.Data, result.Data);   
				}
			}
		}

		class ObjectWithPrivateField
		{
			private int Value;

			public void Set()
			{
				this.Value = 10;
			}

			public int Get()
			{
				return this.Value;
			}
		}

		[Fact]
		public void can_Serialize_object_with_private_field()
		{
			ObjectWithPrivateField obj = new ObjectWithPrivateField();
			obj.Set();

			using (var store = NewDocumentStore())
			{
				store.Conventions.JsonContractResolver = new DefaultContractResolver(true)
				{
					DefaultMembersSearchFlags = BindingFlags.NonPublic|BindingFlags.Public|BindingFlags.Instance
				};
				using (var session = store.OpenSession())
				{
					session.Store(obj, "test");
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result = session.Load<ObjectWithPrivateField>("test");
					Assert.NotNull(result);
					Assert.Equal(obj.Get(), result.Get());
				}
			}
		}
	}
}
