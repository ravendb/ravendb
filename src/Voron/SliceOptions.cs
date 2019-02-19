using Sparrow.Server;

namespace Voron
{
    public enum SliceOptions : byte
    {
        Key = 0,
        BeforeAllKeys = ByteStringType.UserDefined1,
        AfterAllKeys = ByteStringType.UserDefined2,
    }
}
