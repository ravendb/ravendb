using System;
using System.IO;

namespace Voron.Trees
{
    public interface IIterator : IDisposable
    {
        bool Seek(Slice key);
        Slice CurrentKey { get; }
        int GetCurrentDataSize();
        Slice RequiredPrefix { get; set; }
        Slice MaxKey { get; set; }
        bool MoveNext();
		bool MovePrev();
	    void Skip(int count);
        Stream CreateStreamForCurrent();
    }
}