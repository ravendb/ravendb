using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class OsInfo : IDynamicJson
    {
        public OSType OSType { get; set; }

        public string FullName { get; set; }

        public string Version { get; set; }

        public string BuildVersion { get; set; }

        public bool Is32Bits { get; set; }

        public override bool Equals(object obj)
        {
            return Equals(obj as OsInfo);
        }

        private bool Equals(OsInfo other)
        {
            if (other == null)
                return false;

            return OSType == other.OSType &&
                   string.Equals(FullName, other.FullName) &&
                   string.Equals(Version, other.Version) &&
                   string.Equals(BuildVersion, other.BuildVersion) &&
                   Is32Bits == other.Is32Bits;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = OSType.GetHashCode();
                hashCode = (hashCode * 397) ^ (FullName != null ? FullName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Version != null ? Version.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (BuildVersion != null ? BuildVersion.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Is32Bits.GetHashCode();
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(OSType)] = OSType,
                [nameof(FullName)] = FullName,
                [nameof(Version)] = Version,
                [nameof(BuildVersion)] = BuildVersion,
                [nameof(Is32Bits)] = Is32Bits
            };
        }
    }

    public enum OSType
    {
        Windows,
        Linux,
        MacOS
    }
}
