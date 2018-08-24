using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class OsInfo : IDynamicJson
    {
        public OSType Type { get; set; }

        public string FullName { get; set; }

        public string Version { get; set; }

        public string BuildVersion { get; set; }

        public bool Is64Bit { get; set; }

        public override bool Equals(object obj)
        {
            return Equals(obj as OsInfo);
        }

        private bool Equals(OsInfo other)
        {
            if (other == null)
                return false;

            return Type == other.Type &&
                   string.Equals(FullName, other.FullName) &&
                   string.Equals(Version, other.Version) &&
                   string.Equals(BuildVersion, other.BuildVersion) &&
                   Is64Bit == other.Is64Bit;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Type.GetHashCode();
                hashCode = (hashCode * 397) ^ (FullName != null ? FullName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Version != null ? Version.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (BuildVersion != null ? BuildVersion.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Is64Bit.GetHashCode();
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Type)] = Type,
                [nameof(FullName)] = FullName,
                [nameof(Version)] = Version,
                [nameof(BuildVersion)] = BuildVersion,
                [nameof(Is64Bit)] = Is64Bit
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
