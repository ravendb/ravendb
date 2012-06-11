using System;
using System.ComponentModel.Composition.Primitives;
using System.Linq;

namespace Raven.Database.Plugins.Catalogs
{
	public class BundlesFilteredCatalog : FilteredCatalog
	{
		private readonly string[] bundles;

		public BundlesFilteredCatalog(ComposablePartCatalog catalogToFilter, string[] bundles) : base(catalogToFilter)
		{
			this.bundles = bundles;
		}

		protected override bool IsMatch(ExportDefinition exportDefinition)
		{
			object bundle;
			if (exportDefinition.Metadata.TryGetValue("Bundle", out bundle))
			{
				var bundleName = bundle as string;
				if (bundleName != null)
					return bundles.Contains(bundleName, StringComparer.InvariantCultureIgnoreCase);
			}
			return base.IsMatch(exportDefinition);
		}
	}
}