namespace Raven.Client.Http
{
    public class ServerNode
    {
        public string Url;
        public string Database;
        public string ApiKey;
        public string CurrentToken;
        public bool IsFailed;

        private bool Equals(ServerNode other)
        {
            return string.Equals(Url, other.Url) && 
                string.Equals(Database, other.Database) && 
                string.Equals(ApiKey, other.ApiKey);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ServerNode) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Url?.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ (Database?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (ApiKey?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
    }
}