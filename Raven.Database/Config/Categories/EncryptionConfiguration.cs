using System.ComponentModel;
using Raven.Database.Config.Attributes;

namespace Raven.Database.Config.Categories
{
    public class EncryptionConfiguration : ConfigurationCategory
    {
        [Description("Whatever we should use FIPS compliant encryption algorithms")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Encryption/FIPS")]
        public bool UseFips { get; set; }

        [DefaultValue(128)]
        [ConfigurationEntry("Raven/Encryption/KeyBitsPreference")]
        public int EncryptionKeyBitsPreference { get; set; }

        [Description("Whatever we should use SSL for this connection")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/UseSsl")]
        public bool UseSsl { get; set; }

        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Encryption/Algorithm")]
        public string AlgorithmType { get; set; }

        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Encryption/Key")]
        public string EncryptionKey { get; set; }

        [DefaultValue(true)]
        [ConfigurationEntry("Raven/Encryption/EncryptIndexes")]
        public bool EncryptIndexes { get; set; }
    }
}