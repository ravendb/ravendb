using System.Collections.Generic;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class BundlesModel : PageViewModel
	{
		public List<string> AllTabs = new List<string>
		{
			"Selection",
			"Encryption",
			"Quotas",
			"Replication",
			"Versioning"
		};
	}
}
