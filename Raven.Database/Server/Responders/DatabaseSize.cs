using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class DatabaseSize : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/database/size?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			switch (context.Request.HttpMethod)
			{
				case "GET":
					var totalSizeOnDisk = Database.GetTotalSizeOnDisk();
					context.WriteJson(new
					{
						DatabaseSize = totalSizeOnDisk,
						DatabaseSizeHumane = Humane(totalSizeOnDisk)
					});
					break;
			}
		}

		public static string Humane(long? size)
		{
			if (size == null)
				return null;

			var absSize = Math.Abs(size.Value);
			const double GB = 1024 * 1024 * 1024;
			const double MB = 1024 * 1024;
			const double KB = 1024;

			if (absSize > GB) // GB
				return string.Format("{0:#,#.##;;0} GBytes", size / GB);
			if (absSize > MB)
				return string.Format("{0:#,#.##;;0} MBytes", size / MB);
			if (absSize > KB)
				return string.Format("{0:#,#.##;;0} KBytes", size / KB);
			return string.Format("{0:#,#;;0} Bytes", size);

		}
	}
}