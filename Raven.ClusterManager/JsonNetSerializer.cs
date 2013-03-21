// -----------------------------------------------------------------------
//  <copyright file="JsonNetSerializer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using Nancy;
using Nancy.IO;
using Nancy.Responses;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Converters;
using Raven.Imports.Newtonsoft.Json.Serialization;

namespace Raven.ClusterManager
{
	public class JsonNetSerializer : ISerializer
	{
		private readonly DefaultJsonSerializer defaultSerializer = new DefaultJsonSerializer();
		private readonly JsonSerializer serializer;

		public JsonNetSerializer()
		{
			var settings = new JsonSerializerSettings
			{
				ContractResolver = new CamelCasePropertyNamesContractResolver(),
				Converters = {new StringEnumConverter {CamelCaseText = true}},
			};
			serializer = JsonSerializer.Create(settings);
		}

		public bool CanSerialize(string contentType)
		{
			return defaultSerializer.CanSerialize(contentType);
		}

		public void Serialize<TModel>(string contentType, TModel model, Stream outputStream)
		{
			using (var writer = new JsonTextWriter(new StreamWriter(new UnclosableStreamWrapper(outputStream))))
			{
				serializer.Serialize(writer, model);

				writer.Flush();
			}
		}

		public IEnumerable<string> Extensions { get; private set; }
	}
}