using System;

namespace Raven.Database.Data
{
	public class ServerError
	{
		public string Index { get; set; }
		public string Error { get; set; }
		public DateTime Timestamp { get; set; }

		public string Document { get; set; }
	}
}