using System;
using System.IO;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CanReadLuceneProjectedDateTimeOffset 
	{
		[Fact]
		public void Can_read_date_time_offset_from_lucene_query()
		{
			var jsonSerializer = new DocumentConvention().CreateSerializer();
			var deserialize = jsonSerializer.Deserialize<Test>(new JsonTextReader(new StringReader(@"{""Item"": ""20090402193554412""}")));
			Assert.Equal(2009, deserialize.Item.Year);
		}

		private class Test
		{
			public DateTimeOffset Item { get; set; }
		}
	}
}