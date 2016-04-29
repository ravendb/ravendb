// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Linq;

using Raven.Client.Indexes;

using TShirt = SlowTests.Core.Utils.Entities.TShirt;

namespace SlowTests.Core.Utils.Indexes
{
    public class TShirtIndex : AbstractIndexCreationTask<TShirt>
    {
        public class Result
        {
            public string Id { get; set; }
            public string Manufacturer { get; set; }
            public string Color { get; set; }
            public string Size { get; set; }
        }

        public TShirtIndex()
        {
            this.Map = tshirts => from tshirt in tshirts
                                  from type in tshirt.Types
                                  select new
                                  {
                                      Id = tshirt.Id,
                                      Manufacturer = tshirt.Manufacturer,
                                      Color = type.Color,
                                      Size = type.Size
                                      //Types = tshirt.Types.Select(),
                                  };
        }
    }
}
