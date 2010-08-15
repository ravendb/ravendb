using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
	public class MultiLoadResult
	{
		public List<JObject> Results { get; set; }
		public List<JObject> Includes { get; set; }

		public MultiLoadResult()
		{
			Results = new List<JObject>();
			Includes = new List<JObject>();
		}
	}
}