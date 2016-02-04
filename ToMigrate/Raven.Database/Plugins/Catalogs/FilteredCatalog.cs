using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Primitives;
using System.Linq;

namespace Raven.Database.Plugins.Catalogs
{
    public abstract class FilteredCatalog : ComposablePartCatalog
    {
        private static object locker = new object();

        private readonly ComposablePartCatalog catalogToFilter;

        protected FilteredCatalog(ComposablePartCatalog catalogToFilter)
        {
            this.catalogToFilter = catalogToFilter;
        }

        public ComposablePartCatalog CatalogToFilter
        {
            get { return catalogToFilter; }
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
            lock (locker)
            {
                return catalogToFilter
                    .GetExports(definition)
                    .Where(x => IsMatch(x.Item1) && IsMatch(x.Item2))
                    .ToList();
            }
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
