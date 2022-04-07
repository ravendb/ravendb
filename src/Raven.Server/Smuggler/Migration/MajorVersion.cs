using System.ComponentModel;

namespace Raven.Server.Smuggler.Migration
{
    public enum MajorVersion
    {
        Unknown,
        [Description("v2.x")]
        V2,
        [Description("v3.0")]
        V30,
        [Description("v3.5")]
        V35,
        [Description("v4.x")]
        V4,
        [Description("v5.x")]
        V5,
        [Description("v6.x")]
        V6,
        [Description("Greater than current")]
        GreaterThanCurrent
    }
}
