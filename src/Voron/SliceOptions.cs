using Sparrow.Server;

namespace Voron
{
    public enum SliceOptions : byte
    {
        Key = 0,
        BeforeAllKeys = ByteStringType.UserDefined1,
        AfterAllKeys = ByteStringType.UserDefined2,
        OutOfScope = ByteStringType.UserDefined3 // this used so seeking on a tree will return false
    }
}
