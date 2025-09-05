using System.Collections.Generic;

namespace SlowTests.Core.Utils.Entities
{
    public class TShirt
    {
        public string Id { get; set; }
        public int ReleaseYear { get; set; }
        public string Manufacturer { get; set; }
        public List<TShirtType> Types { get; set; }
    }
}