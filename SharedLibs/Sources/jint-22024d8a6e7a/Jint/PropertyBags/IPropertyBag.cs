using System;
using System.Collections.Generic;
using System.Text;
using Jint.Native;

namespace Jint
{
    public interface IPropertyBag : IEnumerable<KeyValuePair<string,Descriptor>>
    {
        Descriptor Put(string name, Descriptor descriptor);
        void Delete(string name);
        Descriptor Get(string name);
        bool TryGet(string name, out Descriptor descriptor);

        int Count { get; }

        IEnumerable<Descriptor> Values { get; }
    }
}
