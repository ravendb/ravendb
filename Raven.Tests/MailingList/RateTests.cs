using System.IO;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class RateTests : RavenTest
	{
		public class Rate
		{
			public decimal Compoundings { get; set; }
		}

		private readonly Rate rate = new Rate { Compoundings = 12.166666666666666666666666667m };

		[Fact]
		public void should_deserialize()
		{
			var serialize = JsonConvert.SerializeObject(rate);
			Assert.Equal(rate.Compoundings, JsonConvert.DeserializeObject<Rate>(serialize).Compoundings);
		}

		[Fact]
		public void should_deserialize_from_store()
		{
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(rate);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(rate.Compoundings, session.Load<Rate>(1).Compoundings);
				}
			}
		}

		[Fact]
		public void should_serializer_from_store_text()
		{
			using (var store = NewDocumentStore())
			{
				var stringWriter = new StringWriter();
				store.Conventions.CreateSerializer().Serialize(stringWriter, rate);
				Assert.Contains("12.166666666666666666666666667", stringWriter.GetStringBuilder().ToString());
			}
		}


		[Fact]
		public void should_serializer_from_store_obj()
		{
			var jTokenWriter = new JTokenWriter();
			new JsonSerializer().Serialize(jTokenWriter, rate);
			Assert.Contains("12.166666666666666666666666667", jTokenWriter.Token.ToString());
		}
		[Fact]
		public void can_restore_from_text()
		{
			var jTokenWriter = new JTokenWriter();
			new JsonSerializer().Serialize(jTokenWriter, rate);
			var rate2 = new JsonSerializer().Deserialize<Rate>(new JTokenReader(jTokenWriter.Token));

			Assert.Equal(rate.Compoundings, rate2.Compoundings);
		}
	}
}