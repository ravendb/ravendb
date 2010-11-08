namespace Raven.Client.Silverlight.Data
{
    using Raven.Client.Silverlight.Common;

    public class IndexField
    {
        public string Name { get; set; }

        public StoreType Store { get; set; }

        public IndexType Index { get; set; }
    }
}
