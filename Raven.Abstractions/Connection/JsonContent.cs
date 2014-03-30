// -----------------------------------------------------------------------
//  <copyright file="JsonContent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Connection
{
	public class JsonContent : HttpContent
	{
		private static readonly Encoding DefaultEncoding = new UTF8Encoding(false);

		public JsonContent(RavenJToken data = null)
		{
			Data = data;
		}

		public RavenJToken Data { get; set; }

		public string Jsonp { get; set; }

		public bool IsOutputHumanReadable { get; set; }

		protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
		    if (HasNoData())
		        return new CompletedTask<bool>(true);

			if (string.IsNullOrEmpty(Jsonp))
				Headers.ContentType = new MediaTypeHeaderValue("application/json") {CharSet = "utf-8"};
			else
				Headers.ContentType = new MediaTypeHeaderValue("application/javascript") {CharSet = "utf-8"};

			var writer = new StreamWriter(stream, DefaultEncoding);
			if (string.IsNullOrEmpty(Jsonp) == false)
			{
				writer.Write(Jsonp);
				writer.Write("(");
			}

			Data.WriteTo(new JsonTextWriter(writer)
			{
				Formatting = IsOutputHumanReadable ? Formatting.Indented : Formatting.None,
			}, Default.Converters);

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

		public JsonContent WithRequest(HttpRequestMessage request)
		{
			if (request != null)
			{
				// Just a directly request from a browser should return human readable JSON, but not a request from a JavaScript application.
				IsOutputHumanReadable = request.Headers.Accept.Any() && request.Headers.UserAgent.Any() && request.Headers.Referrer == null;
			}
			return this;
		}
	}
}