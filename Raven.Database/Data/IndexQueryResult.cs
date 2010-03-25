using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
	public class IndexQueryResult
	{
		public string Key { get; set; }
		public JObject Projection { get; set; }
	}
}