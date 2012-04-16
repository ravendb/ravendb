using System;
using System.Collections.Generic;
using System.IO;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DictionaryOfDateTime 
	{
		public class WithDic
		{
			public Dictionary<DateTime, int> Items { get; set; }
		}
		
		[Fact]
		public void CanBeSerializedProperly()
		{
			var jsonSerializer = new DocumentConvention().CreateSerializer();
			var stringWriter = new StringWriter();

			var item = new WithDic
			{
				Items = new Dictionary<DateTime, int>
				{
					{new DateTime(2011, 11, 24), 1}
				}
			};

			jsonSerializer.Serialize(stringWriter, item);

			var s = stringWriter.GetStringBuilder().ToString();
			Assert.Equal("{\"Items\":{\"2011-11-24T00:00:00.0000000\":1}}", s);
		}

		[Fact]
		public void CanBeDeSerializedProperly()
		{
			var jsonSerializer = new DocumentConvention().CreateSerializer();
			var stringWriter = new StringWriter();

			var item = new WithDic
			{
				Items = new Dictionary<DateTime, int>
				{
					{new DateTime(2011, 11, 24), 1}
				}
			};

			jsonSerializer.Serialize(stringWriter, item);

			var s = stringWriter.GetStringBuilder().ToString();
			var withDic = jsonSerializer.Deserialize<WithDic>(new JsonTextReader(new StringReader(s)));

			Assert.Equal(1, withDic.Items[new DateTime(2011, 11, 24)]);
		}
	}
}