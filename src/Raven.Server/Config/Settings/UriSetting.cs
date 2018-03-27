using System;

namespace Raven.Server.Config.Settings
{
    public struct UriSetting
    {
        public readonly string UriValue;

        public UriSetting(string uri)
        {
            UriValue = uri;

            if (string.IsNullOrWhiteSpace(UriValue))
            {
                UriValue = null;
                return;
            }

            if (Uri.TryCreate(uri, UriKind.RelativeOrAbsolute, out _) == false)
                throw new ArgumentException($"{uri} is not a valid URI.");
        }

        public override string ToString()
        {
            return UriValue;
        }
    }
}
