using Rachis.Commands;
using Sparrow.Collections.LockFree;

namespace RachisTests
{
    public abstract class DictionaryCommand : Command
    {
        public abstract void Apply(ConcurrentDictionary<string, int> data);

        public string Key { get; set; }

        public class Set : DictionaryCommand
        {
            public int Value { get; set; }

            public override void Apply(ConcurrentDictionary<string, int> data)
            {
                data[Key] = Value;
            }
        }

        public class Inc : DictionaryCommand
        {
            public int Value { get; set; }

            public override void Apply(ConcurrentDictionary<string, int> data)
            {
                int value;
                data.TryGetValue(Key, out value);
                data[Key] = value + Value;
            }
        }


        public class Del : DictionaryCommand
        {
            public override void Apply(ConcurrentDictionary<string, int> data)
            {
                int value;
                data.TryRemove(Key, out value);
            }
        }
    }
}
