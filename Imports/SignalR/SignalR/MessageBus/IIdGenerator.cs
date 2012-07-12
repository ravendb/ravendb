using System;

namespace Raven.Imports.SignalR
{
    public interface IIdGenerator<T> where T : IComparable<T>
    {
        T GetNext();
        T ConvertFromString(string value);
        string ConvertToString(T value);
    }
}
