using System;

namespace Raven.Abstractions.Data
{
	public class QueryHeaderInformation
	{
		public string Index { get; set; }
		public bool IsStable { get; set; }
		public DateTime IndexTimestamp { get; set; }
		public int TotalResults { get; set; }
		public Etag ResultEtag { get; set; }
		public Etag IndexEtag { get; set; }
	}
}