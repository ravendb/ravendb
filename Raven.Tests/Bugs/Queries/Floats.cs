using System.IO;
using System.Linq;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class Floats : RavenTest
	{
		public class FloatValue
		{
			public int Id { get; set; }
			public float Value { get; set; }
		}

		[Fact]
		public void Basic()
		{
			var ravenJObject = new RavenJObject
			{
				{"Val", 3.3f}
			};
			var s = ravenJObject.ToString(Formatting.None);
			Assert.Equal("{\"Val\":3.3}", s);
		}

		[Fact]
		public void FromObject()
		{
			var ravenJObject = RavenJObject.FromObject(new FloatValue
			{
				Value = 3.3f
			});
			var s = ravenJObject.ToString(Formatting.None);
			Assert.Equal("{\"Id\":0,\"Value\":3.3}", s);
		}

		[Fact]
		public void WithBinaryWriter()
		{
			var ms = new MemoryStream();
			var binarWriter = new BinaryWriter(ms);
			binarWriter.Write(3.3f);
			ms.Position = 0;
			Assert.Equal(4, ms.Length);

			var reader = new BinaryReader(ms);
			var readSingle = reader.ReadSingle();
			Assert.Equal(3.3f, readSingle);
		}

		[Fact]
		public void WithBson()
		{
			var ravenJObject = RavenJObject.FromObject(new FloatValue
			{
				Value = 3.3f
			});
			var memoryStream = new MemoryStream();
			ravenJObject.WriteTo(new BsonWriter(memoryStream));
			memoryStream.Position = 0;
			ravenJObject = RavenJObject.Load(new BsonReader(memoryStream));


			var s = ravenJObject.ToString(Formatting.None);
			Assert.Equal("{\"Id\":0,\"Value\":3.3}", s);
		}


		[Fact]
		public void Query()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new FloatValue
					              	{
					              		Id = 1,
					              		Value = 3.3f
					              	});
					session.SaveChanges();
				}
				WaitForUserToContinueTheTest(documentStore);
				using (var session = documentStore.OpenSession())
				{

					var results = session.Query<FloatValue>()
						.Where(x => x.Value == 3.3f)
						.Customize(x => x.WaitForNonStaleResults());

					Assert.True(results.Count() == 1);
				}
			}
		}

		[Fact]
		public void Persistence()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new FloatValue
					{
						Id = 1,
						Value = 3.3f
					});
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var value = session.Load<FloatValue>(1);
					Assert.Equal(3.3f, value.Value);
				}
			}
		}
	}
}
