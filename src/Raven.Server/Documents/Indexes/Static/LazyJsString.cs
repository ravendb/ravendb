using Jint.Native;
using Jint.Runtime;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static;

internal sealed class LazyJsString : JsString
{
    internal readonly LazyStringValue _lazyValue;

    public LazyJsString(LazyStringValue lazyValue) : base(null)
    {
        _lazyValue = lazyValue;
    }

    public override string ToString()
    {
        return _lazyValue.ToString();
    }

    public override bool Equals(JsString obj)
    {
        return obj switch
        {
            LazyJsString lazyJsString => _lazyValue.Equals(lazyJsString._lazyValue),
            LazyCompressedJsString lazyCompressedJsString => _lazyValue.Equals(lazyCompressedJsString._lazyValue.ToLazyStringValue()),
            _ => obj is not null && _lazyValue.Equals(obj.ToString())
        };
    }

    public override bool IsLooselyEqual(JsValue value)
    {
        return value switch
        {
            LazyJsString lazyJsString => _lazyValue.Equals(lazyJsString._lazyValue),
            LazyCompressedJsString lazyCompressedJsString => _lazyValue.Equals(lazyCompressedJsString._lazyValue.ToLazyStringValue()),
            _ => value is not null && _lazyValue.Equals(TypeConverter.ToString(value)),
        };
    }

    public override int GetHashCode()
    {
        return _lazyValue.GetHashCode();
    }

    public override char this[int index] => _lazyValue.ToString()[index];

    public override int Length => _lazyValue.Length;
}
