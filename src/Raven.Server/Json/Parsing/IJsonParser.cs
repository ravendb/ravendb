using System;

namespace Raven.Server.Json.Parsing
{
    public interface IJsonParser : IDisposable
    {
        void Read();
        void ValidateFloat();
    }
}