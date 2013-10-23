// -----------------------------------------------------------------------
//  <copyright file="JsonContent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class JsonContent : HttpContent
	{
		private readonly Lazy<byte[]> lazyContent;

		public JsonContent(RavenJToken token = null)
		{
			Token = token;
			lazyContent = new Lazy<byte[]>(() => TokenAsByteArray(Token, Jsonp));
		}

		public RavenJToken Token { get; set; }

		public string Jsonp { get; set; }

		private static byte[] TokenAsByteArray(RavenJToken token, string jsonp)
		{
			if (token == null)
			{
				return new byte[0];
			}
			var memoryStream = new MemoryStream();
			using (var writer = new StreamWriter(memoryStream))
			{
				if (string.IsNullOrEmpty(jsonp) == false)
				{
					writer.Write(jsonp);
					writer.Write("(");
				}
				token.WriteTo(new JsonTextWriter(writer), Default.Converters);
				if (string.IsNullOrEmpty(jsonp) == false)
				{
					writer.Write(")");
				}
			}
			return memoryStream.ToArray();
		}

		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			if (lazyContent.Value.Length == 0)
				return;

			await stream.WriteAsync(lazyContent.Value, 0, lazyContent.Value.Length);
		}

		protected override bool TryComputeLength(out long length)
		{
			length = lazyContent.Value.Length;
			return true;
		}
	}
}