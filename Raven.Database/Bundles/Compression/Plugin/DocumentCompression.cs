using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Bundles.Compression.Streams;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Compression.Plugin
{
	[InheritedExport(typeof(AbstractDocumentCodec))]
	[ExportMetadata("Order", 500)]
	[ExportMetadata("Bundle", "Compression")]
	public class DocumentCompression : AbstractDocumentCodec
	{
		[CLSCompliant(false)]
		public const uint CompressFileMagic = 0x72706D43; // "Cmpr"

		public override Stream Encode(string key, RavenJObject data, RavenJObject metadata, Stream dataStream)
		{
			return new CompressStream(dataStream);
		}

		public override Stream Decode(string key, RavenJObject metadata, Stream dataStream)
		{
			return new DecompressStream(dataStream);
		}
	}
}
