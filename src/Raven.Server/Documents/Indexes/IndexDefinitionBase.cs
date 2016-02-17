namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexDefinitionBase
    {
        protected IndexDefinitionBase(string name, string[] collections)
        {
            Name = name;
            Collections = collections;
        }

        public string Name { get; private set; }

        public string[] Collections { get; private set; }

        public abstract IndexField[] MapFields { get; }
    }
}