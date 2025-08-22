using System;

namespace Sparrow.Json.Parsing
{
    public interface IJsonParser : IDisposable
    {
        bool Read();
        void ValidateFloat();
        string GenerateErrorState();
        OnStringReadDelegate OnStringRead { set; }
    }
    public delegate void OnStringReadDelegate(UnmanagedWriteBuffer buffer, bool partial);
}
