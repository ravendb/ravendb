using System;
using System.Threading.Tasks;

namespace Raven.Server.Json.Parsing
{
    public interface IJsonParser : IDisposable
    {
        Task ReadAsync();
        void ValidateFloat();
    }
}