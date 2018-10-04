using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public interface IPropertyAccessor
    {
        IEnumerable<(string Key, object Value, CompiledIndexField GroupByField, bool IsGroupByField)> GetPropertiesInOrder(object target);

        object GetValue(string name, object target);
    }
}
