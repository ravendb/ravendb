using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Data;
using Raven.Abstractions.Data;

namespace Raven.Studio.Features.Documents
{
    /// <summary>
    /// Given a list of ColumnDefinitions, will extract the values specified by the binding of each column definition from a ViewableDocument
    /// </summary>
    /// <remarks>
    /// Must be called on the UI Thread, because it uses FrameworkObjects to do the value extraction
    /// </remarks>
    internal class DocumentColumnsExtractor
    {
        private IList<Func<JsonDocument, object>> valueExtractors;

        public DocumentColumnsExtractor(IList<ColumnDefinition> columns)
        {
            valueExtractors = columns.Select(c => c.GetValueExtractor()).ToArray();
        }

        public IEnumerable<object> GetValues(JsonDocument document)
        {
            var values = valueExtractors.Select(e => e(document));

            return values;
        } 

    }
}