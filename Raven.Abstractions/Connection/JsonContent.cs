// -----------------------------------------------------------------------
//  <copyright file="JsonContent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Connection
{
	public class JsonContent : HttpContent
	{
		public JsonContent(RavenJToken data = null)
		{
			Data = data;
		}

		public RavenJToken Data { get; set; }

		public string Jsonp { get; set; }

		protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{


		    if (HasNoData())
		        return new CompletedTask<bool>(true);

			if (string.IsNullOrEmpty(Jsonp))
				Headers.ContentType = new MediaTypeHeaderValue("application/json") {CharSet = "utf-8"};
			else
				Headers.ContentType = new MediaTypeHeaderValue("application/javascript") {CharSet = "utf-8"};

			var writer = new StreamWriter(stream);
			if (string.IsNullOrEmpty(Jsonp) == false)
			{
				writer.Write(Jsonp);
				writer.Write("(");
			}

			Data.WriteTo(new JsonTextWriter(writer), Default.Converters);

			if (string.IsNullOrEmpty(Jsonp) == false)
				writer.Write(")");

			writer.Flush();
            return new CompletedTask<bool>(true);
		}

		private bool HasNoData()
		{
			return Data == null || Data.Type == JTokenType.Null;
		}

		protected override bool TryComputeLength(out long length)
		{
			var hasNoData = HasNoData();
			length = hasNoData ? 0 : -1;
			return hasNoData;
		}
	}
}