using System;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class DataInfo
	{
		public string Name { get; set; }
		public DateTime CreatedAt { get; set; }
		public long Length { get; set; }
	}
}