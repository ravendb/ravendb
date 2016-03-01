using System;
using System.Threading.Tasks;

namespace Raven.Server.Json.Parsing
{
    public interface IJsonParser : IDisposable
    {
        bool Read();
        void ValidateFloat();
    }
}