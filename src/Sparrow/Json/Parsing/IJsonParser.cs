using System;

namespace Sparrow.Json.Parsing
{
    public interface IJsonParser : IDisposable
    {
        bool Read();
        void ValidateFloat();
    }
}