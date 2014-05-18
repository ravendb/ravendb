using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Raven.Database.Server.Controllers
{
	public class StaticHeadContent : HttpContent
	{
		public StaticHeadContent(int size)
		{
			AttachmentSize = size;
			Headers.ContentLength = AttachmentSize;
		}

		public int AttachmentSize { get; set; }

		protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			return Task.FromResult<object>(null);
		}

		protected override bool TryComputeLength(out long length)
		{
			length = AttachmentSize;
			return true;
		}
	}
}