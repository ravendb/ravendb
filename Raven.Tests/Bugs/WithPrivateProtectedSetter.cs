//-----------------------------------------------------------------------
// <copyright file="WithPrivateProtectedSetter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class WithPrivateProtectedSetter
	{

		[Fact]
		public void CanSerializeToJsonCorrectly()
		{
			var serializer = new DocumentConvention().CreateSerializer();
			var stringWriter = new StringWriter();
			serializer.Serialize(stringWriter, new Company("Hibernating Rhinos", "Middle East"));
			var deserializeObject = serializer.Deserialize<Company>(new JsonTextReader(new StringReader(stringWriter.GetStringBuilder().ToString())));
			Assert.Equal("Hibernating Rhinos", deserializeObject.Name);
			Assert.Equal("Middle East", deserializeObject.Region);
		}

		[Fact]
		public void WillNotSerializeFields()
		{
			var serializer = new DocumentConvention().CreateSerializer();
			var stringWriter = new StringWriter();
			serializer.Serialize(stringWriter, new Company("Hibernating Rhinos", "Middle East"));
			var s = stringWriter.GetStringBuilder().ToString();
			Assert.DoesNotContain("k__BackingField", s);
		}

		public class Company
		{
			public Company()
			{
				
			}

			public Company(string name, string region)
			{
				Name = name;
				Region = region;
			}

			public string Name { get; private set; }
			public string Region { get; protected set; }
		}
	}
}
