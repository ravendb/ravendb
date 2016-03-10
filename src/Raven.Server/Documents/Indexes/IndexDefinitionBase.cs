using System.Collections.Generic;
using System.Linq;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexDefinitionBase
    {
        protected static readonly Slice DefinitionSlice = "Definition";

        private readonly Dictionary<string, IndexField> _fieldsByName;

        protected IndexDefinitionBase(string name, string[] collections, IndexField[] mapFields)
        {
            Name = name;
            Collections = collections;
            MapFields = mapFields;

            _fieldsByName = MapFields.ToDictionary(x => x.Name, x => x);
        }

        public string Name { get; set; }

        public string[] Collections { get; set; }

        public IndexField[] MapFields { get; set; }

        public abstract void Persist(TransactionOperationContext context);

        public bool ContainsField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return _fieldsByName.ContainsKey(field);
        }

        public IndexField GetField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return _fieldsByName[field];
        }
    }
}