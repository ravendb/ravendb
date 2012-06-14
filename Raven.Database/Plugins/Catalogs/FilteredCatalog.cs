using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Primitives;
using System.Linq;

namespace Raven.Database.Plugins.Catalogs
{
	public abstract class FilteredCatalog : ComposablePartCatalog
	{
		private readonly ComposablePartCatalog catalogToFilter;

		protected FilteredCatalog(ComposablePartCatalog catalogToFilter)
		{
			this.catalogToFilter = catalogToFilter;
		}

		protected override void Dispose(bool disposing)
		{
			catalogToFilter.Dispose();
			base.Dispose(disposing);
		}

		public override IQueryable<ComposablePartDefinition> Parts
		{
			get
			{
				return from part in catalogToFilter.Parts
					   from exportDefinition in part.ExportDefinitions
					   where IsMatch(part) && IsMatch(exportDefinition)
					   select part;
			}
		}

		public override IEnumerable<Tuple<ComposablePartDefinition, ExportDefinition>> GetExports(ImportDefinition definition)
		{
			return from export in catalogToFilter.GetExports(definition)
				   where IsMatch(export.Item1) && IsMatch(export.Item2)
				   select export;
		}

		protected virtual bool IsMatch(ComposablePartDefinition composablePartDefinition)
		{
			return true;
		}

		protected virtual bool IsMatch(ExportDefinition exportDefinition)
		{
			return true;
		}
	}
}