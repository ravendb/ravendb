using Jint.Native;
using Jint.Runtime;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static;

internal sealed class LazyCompressedJsString : JsString
{
    internal readonly LazyCompressedStringValue _lazyValue;

    public LazyCompressedJsString(LazyCompressedStringValue lazyValue) : base(null)
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
            LazyCompressedJsString lazyCompressedJsString => _lazyValue.Equals(lazyCompressedJsString._lazyValue),
            LazyJsString lazyJsString => _lazyValue.Equals(lazyJsString._lazyValue),
            _ => obj is not null && _lazyValue.Equals(obj.ToString())
        };
    }

    public override bool IsLooselyEqual(JsValue value)
    {
        return value switch
        {
            LazyCompressedJsString lazyCompressedJsString => _lazyValue.Equals(lazyCompressedJsString._lazyValue),
            LazyJsString lazyJsString => _lazyValue.Equals(lazyJsString._lazyValue),
            _ => value is not null && _lazyValue.Equals(TypeConverter.ToString(value)),
        };
    }

    public override int GetHashCode()
    {
        return _lazyValue.GetHashCode();
    }

    public override char this[int index] => _lazyValue.ToString()[index];

    public override int Length => _lazyValue.UncompressedSize;
}
