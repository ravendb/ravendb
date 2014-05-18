using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Raven.Abstractions.RavenFS;
using Raven.Abstractions.Util;

namespace Raven.Database.Server.RavenFS.Synchronization.Multipart
{
	public class SeedFilePart : HttpContent
	{
		public SeedFilePart(long from, long to)
		{
			Headers.ContentDisposition = new ContentDispositionHeaderValue("form-data");
			Headers.ContentDisposition.Parameters.Add(new NameValueHeaderValue(SyncingMultipartConstants.NeedType,
																			   SyncingNeedType));
			Headers.ContentDisposition.Parameters.Add(new NameValueHeaderValue(SyncingMultipartConstants.RangeFrom,
																			   @from.ToString(CultureInfo.InvariantCulture)));
			Headers.ContentDisposition.Parameters.Add(new NameValueHeaderValue(SyncingMultipartConstants.RangeTo,
																			   to.ToString(CultureInfo.InvariantCulture)));

			Headers.ContentType = new MediaTypeHeaderValue("plain/text");
		}

		public string SyncingNeedType
		{
			get { return "seed"; }
		}

		protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			return new CompletedTask();
		}

		protected override bool TryComputeLength(out long length)
		{
			length = 0;
			return true;
		}
	}
}