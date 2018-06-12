using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class BuildNumber : IDynamicJson
    {
        public string ProductVersion { get; set; }

        public int BuildVersion { get; set; }

        public string CommitHash { get; set; }

        public string FullVersion { get; set; }

        public override bool Equals(object obj)
        {
            return Equals(obj as BuildNumber);
        }

        private bool Equals(BuildNumber other)
        {
            if (other == null)
                return false;

            return string.Equals(ProductVersion, other.ProductVersion) && 
                   BuildVersion == other.BuildVersion && 
                   string.Equals(CommitHash, other.CommitHash) && 
                   string.Equals(FullVersion, other.FullVersion);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (ProductVersion != null ? ProductVersion.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ BuildVersion;
                hashCode = (hashCode * 397) ^ (CommitHash != null ? CommitHash.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (FullVersion != null ? FullVersion.GetHashCode() : 0);
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ProductVersion)] = ProductVersion,
                [nameof(BuildVersion)] = BuildVersion,
                [nameof(CommitHash)] = CommitHash,
                [nameof(FullVersion)] = FullVersion
            };
        }
    }
}
