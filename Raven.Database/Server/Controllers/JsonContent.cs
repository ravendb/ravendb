// -----------------------------------------------------------------------
//  <copyright file="JsonContent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class JsonContent : HttpContent
	{
		public JsonContent(RavenJToken token = null)
		{
			Token = token;
		}

		public RavenJToken Token { get; set; }
		public string Jsonp { get; set; }

		protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			if (Token == null)
				return Task.FromResult(true);

			var writer = new StreamWriter(stream);
			if (string.IsNullOrEmpty(Jsonp) == false)
			{
				writer.Write(Jsonp);
				writer.Write("(");
			}

			Token.WriteTo(new JsonTextWriter(writer), Default.Converters);

			if (string.IsNullOrEmpty(Jsonp) == false)
				writer.Write(")");

			writer.Flush();
			return Task.FromResult(true);
		}

		protected override bool TryComputeLength(out long length)
		{
			length = 0;
			return Token == null;
		}
	}
}