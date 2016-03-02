using Raven.Abstractions.Indexing;

namespace Raven.Server.Documents.Indexes.Auto
{
    public static class AutoIndexField 
    {
        public static IndexField CreateAutoIndexField(string name)
        {
            return new IndexField
            {
                Name = name,
                Highlighted = false,
                Storage = FieldStorage.No
            };
        }
    }
}