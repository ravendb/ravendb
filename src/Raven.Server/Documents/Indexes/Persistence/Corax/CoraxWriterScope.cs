using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.Enumerators;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxWriterScope
    {
        //our list map for writing enumerable.
        private List<string>[] _stringFieldMap;
        private bool _isEnumerable;

        public CoraxWriterScope(int fieldCount)
        {
            _stringFieldMap = new List<string>[fieldCount];
            _isEnumerable = false;
        }

        /// <returns>True if allocated, false when already exist (we would like to skip nested arrays for now).</returns>
        public bool Allocate(int fieldId, bool isNumeric)
        {
            if (_stringFieldMap[fieldId] != null)
            {
                return false;
            }

            _isEnumerable = true;
            _stringFieldMap[fieldId] = new();
            return true;
        }

        public void WriteCollection(int fieldId, ref IndexEntryWriter entryWriter)
        {
            _isEnumerable = false;
            entryWriter.Write(fieldId, new ArrayIterator(_stringFieldMap[fieldId]));
        }

        public void Write(int fieldId, string value, ref IndexEntryWriter entryWriter)
        {
            if (_isEnumerable)
                _stringFieldMap[fieldId].Add(value);
            else
                entryWriter.Write(fieldId, Encoding.UTF8.GetBytes(value));
        }

        public void Write(int fieldId, string value, long @long, double @double, ref IndexEntryWriter entryWriter)
        {
            if (_isEnumerable)
                _stringFieldMap[fieldId].Add(value);
            else
                entryWriter.Write(fieldId, Encoding.UTF8.GetBytes(value), @long, @double);
        }
    }
}
