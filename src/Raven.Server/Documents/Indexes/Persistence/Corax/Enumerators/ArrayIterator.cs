using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.Enumerators
{
    public struct ArrayIterator : IReadOnlySpanEnumerator
    {
        private List<string> _data;

        public ArrayIterator(List<string> data)
        {
            _data = data;
        }

        public void Add(string value) => _data.Add(value);


        public int Length
        {
            get
            {
                return _data.Count;
            }
        }

        public ReadOnlySpan<byte> this[int i] => Encoding.UTF8.GetBytes(_data[i]);
    }
}
