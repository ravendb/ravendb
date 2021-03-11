using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class IndexingSpeed : AbstractDashboardNotification
    {
        public override DashboardNotificationType Type => DashboardNotificationType.IndexingSpeed;

        public List<IndexingSpeedItem> Items { get; set; }

        public IndexingSpeed()
        {
            Items = new List<IndexingSpeedItem>();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));
            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            var items = new DynamicJsonArray();
            foreach (var indexingSpeedItem in Items)
            {
                if (filter(indexingSpeedItem.Database, requiresWrite: false))
                {
                    items.Add(indexingSpeedItem.ToJson());
                }
            }

            if (items.Count == 0)
                return null;

            var json = base.ToJson();
            json[nameof(Items)] = items;
            return json;
        }
    }

    public class IndexingSpeedItem : IDynamicJson
    {
        public string Database { get; set; }

        public double IndexedPerSecond { get; set; }

        public double MappedPerSecond { get; set; }

        public double ReducedPerSecond { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(IndexedPerSecond)] = IndexedPerSecond,
                [nameof(MappedPerSecond)] = MappedPerSecond,
                [nameof(ReducedPerSecond)] = ReducedPerSecond
            };
        }
    }
}
