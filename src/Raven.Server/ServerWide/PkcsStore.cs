using System;
using System.Collections;
using System.Globalization;
using System.IO;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.Pkcs;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Utilities;
using Org.BouncyCastle.Utilities.Collections;
using Org.BouncyCastle.Utilities.Encoders;
using Org.BouncyCastle.X509;

namespace Raven.Server.ServerWide
{
    // Workaround for https://github.com/dotnet/corefx/issues/30946
    // This class is a partial copy of BouncyCastle's Pkcs12Store which doesn't throw the exception: "attempt to add existing attribute with different value".

    // Explanation: Certificates which were exported in Linux were changed to include a multiple bag attribute (localKeyId).
    // When using BouncyCastle's Pkcs12Store to load the cert, we got an exception: "attempt to add existing attribute with different value",
    // but we don't care about that. We just need to extract the private key when using Load().

    public class PkcsStore
    {
        private static IgnoresCaseHashtable keys = new IgnoresCaseHashtable();
        private static IDictionary localIds = new Hashtable();
        private static IgnoresCaseHashtable certs = new IgnoresCaseHashtable();
        private static IDictionary chainCerts = new Hashtable();
        private static IDictionary keyCerts = new Hashtable();
        private static AsymmetricKeyEntry unmarkedKeyEntry = null;

        public void Load(Stream input, char[] password)
        {
            Asn1Sequence obj = (Asn1Sequence)Asn1Object.FromStream(input);
            Pfx bag = new Pfx(obj);
            ContentInfo info = bag.AuthSafe;
            bool wrongPkcs12Zero = false;

            IList certBags = new ArrayList();
            keys.Clear();
            localIds.Clear();
            unmarkedKeyEntry = null;

            if (info.ContentType.Equals(PkcsObjectIdentifiers.Data))
            {
                byte[] octs = ((Asn1OctetString)info.Content).GetOctets();
                AuthenticatedSafe authSafe = new AuthenticatedSafe(
                    (Asn1Sequence)Asn1OctetString.FromByteArray(octs));
                ContentInfo[] cis = authSafe.GetContentInfo();

                foreach (ContentInfo ci in cis)
                {
                    DerObjectIdentifier oid = ci.ContentType;

                    byte[] octets = null;
                    if (oid.Equals(PkcsObjectIdentifiers.Data))
                    {
                        octets = ((Asn1OctetString)ci.Content).GetOctets();
                    }
                    else if (oid.Equals(PkcsObjectIdentifiers.EncryptedData))
                    {
                        if (password != null)
                        {
                            EncryptedData d = EncryptedData.GetInstance(ci.Content);
                            octets = CryptPbeData(false, d.EncryptionAlgorithm,
                                password, wrongPkcs12Zero, d.Content.GetOctets());
                        }
                    }

                    if (octets != null)
                    {
                        Asn1Sequence seq = (Asn1Sequence)Asn1Object.FromByteArray(octets);

                        foreach (Asn1Sequence subSeq in seq)
                        {
                            SafeBag b = new SafeBag(subSeq);

                            if (b.BagID.Equals(PkcsObjectIdentifiers.CertBag))
                            {
                                certBags.Add(b);
                            }
                            else if (b.BagID.Equals(PkcsObjectIdentifiers.Pkcs8ShroudedKeyBag))
                            {
                                LoadPkcs8ShroudedKeyBag(EncryptedPrivateKeyInfo.GetInstance(b.BagValue),
                                    b.BagAttributes, password, wrongPkcs12Zero);
                            }
                            else if (b.BagID.Equals(PkcsObjectIdentifiers.KeyBag))
                            {
                                LoadKeyBag(PrivateKeyInfo.GetInstance(b.BagValue), b.BagAttributes);
                            }
                            else
                            {
                                // TODO Other bag types
                            }
                        }
                    }
                }
            }


            foreach (SafeBag b in certBags)
            {
                CertBag certBag = new CertBag((Asn1Sequence)b.BagValue);
                byte[] octets = ((Asn1OctetString)certBag.CertValue).GetOctets();
                Org.BouncyCastle.X509.X509Certificate cert = new X509CertificateParser().ReadCertificate(octets);

                //
                // set the attributes
                //
                IDictionary attributes = new Hashtable();
                Asn1OctetString localId = null;
                string alias = null;

                if (b.BagAttributes != null)
                {
                    foreach (Asn1Sequence sq in b.BagAttributes)
                    {
                        DerObjectIdentifier aOid = DerObjectIdentifier.GetInstance(sq[0]);
                        Asn1Set attrSet = Asn1Set.GetInstance(sq[1]);

                        if (attrSet.Count > 0)
                        {
                            // TODO We should be adding all attributes in the set
                            Asn1Encodable attr = attrSet[0];

                            // TODO We might want to "merge" attribute sets with
                            // the same OID - currently, differing values give an error
                            if (attributes.Contains(aOid.Id))
                            {
                                // OK, but the value has to be the same
                                if (!attributes[aOid.Id].Equals(attr))
                                {
                                    //throw new IOException("attempt to add existing attribute with different value");
                                }
                            }
                            else
                            {
                                attributes.Add(aOid.Id, attr);
                            }

                            if (aOid.Equals(PkcsObjectIdentifiers.Pkcs9AtFriendlyName))
                            {
                                alias = ((DerBmpString)attr).GetString();
                            }
                            else if (aOid.Equals(PkcsObjectIdentifiers.Pkcs9AtLocalKeyID))
                            {
                                localId = (Asn1OctetString)attr;
                            }
                        }
                    }
                }

                CertId certId = new CertId(cert.GetPublicKey());
                X509CertificateEntry certEntry = new X509CertificateEntry(cert, attributes);

                chainCerts[certId] = certEntry;

                if (unmarkedKeyEntry != null)
                {
                    if (keyCerts.Count == 0)
                    {
                        string name = Hex.ToHexString(certId.Id);

                        keyCerts[name] = certEntry;
                        keys[name] = unmarkedKeyEntry;
                    }
                }
                else
                {
                    if (localId != null)
                    {
                        string name = Hex.ToHexString(localId.GetOctets());

                        keyCerts[name] = certEntry;
                    }

                    if (alias != null)
                    {
                        // TODO There may have been more than one alias
                        certs[alias] = certEntry;
                    }
                }
            }
        }

        public AsymmetricKeyEntry GetKey(
            string alias)
        {
            if (alias == null)
                throw new ArgumentNullException("alias");

            return (AsymmetricKeyEntry)keys[alias];
        }

        public IEnumerable Aliases
        {
            get { return new EnumerableProxy(GetAliasesTable().Keys); }
        }

        private IDictionary GetAliasesTable()
        {
            IDictionary tab = new Hashtable();

            foreach (string key in certs.Keys)
            {
                tab[key] = "cert";
            }

            foreach (string a in keys.Keys)
            {
                if (tab[a] == null)
                {
                    tab[a] = "key";
                }
            }

            return tab;
        }

        private byte[] CryptPbeData(
            bool forEncryption,
            AlgorithmIdentifier algId,
            char[] password,
            bool wrongPkcs12Zero,
            byte[] data)
        {
            IBufferedCipher cipher = PbeUtilities.CreateEngine(algId.Algorithm) as IBufferedCipher;

            if (cipher == null)
                throw new Exception("Unknown encryption algorithm: " + algId.Algorithm);

            Pkcs12PbeParams pbeParameters = Pkcs12PbeParams.GetInstance(algId.Parameters);
            ICipherParameters cipherParams = PbeUtilities.GenerateCipherParameters(
                algId.Algorithm, password, wrongPkcs12Zero, pbeParameters);
            cipher.Init(forEncryption, cipherParams);
            return cipher.DoFinal(data);
        }

        private static SubjectKeyIdentifier CreateSubjectKeyID(
            AsymmetricKeyParameter pubKey)
        {
            return new SubjectKeyIdentifier(
                SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(pubKey));
        }

        private class IgnoresCaseHashtable
            : IEnumerable
        {
            private readonly IDictionary orig = new Hashtable();
            private readonly IDictionary keys = new Hashtable();

            public void Clear()
            {
                orig.Clear();
                keys.Clear();
            }

            public IEnumerator GetEnumerator()
            {
                return orig.GetEnumerator();
            }

            public ICollection Keys
            {
                get { return orig.Keys; }
            }

            public object Remove(
                string alias)
            {
                string upper = alias.ToUpper(CultureInfo.InvariantCulture);
                string k = (string)keys[upper];

                if (k == null)
                    return null;

                keys.Remove(upper);

                object o = orig[k];
                orig.Remove(k);
                return o;
            }

            public object this[
                string alias]
            {
                get
                {
                    string upper = alias.ToUpper(CultureInfo.InvariantCulture);
                    string k = (string)keys[upper];

                    if (k == null)
                        return null;

                    return orig[k];
                }
                set
                {
                    string upper = alias.ToUpper(CultureInfo.InvariantCulture);
                    string k = (string)keys[upper];
                    if (k != null)
                    {
                        orig.Remove(k);
                    }
                    keys[upper] = alias;
                    orig[alias] = value;
                }
            }

            public ICollection Values
            {
                get { return orig.Values; }
            }
        }

        private void LoadPkcs8ShroudedKeyBag(EncryptedPrivateKeyInfo encPrivKeyInfo, Asn1Set bagAttributes,
            char[] password, bool wrongPkcs12Zero)
        {
            if (password != null)
            {
                PrivateKeyInfo privInfo = PrivateKeyInfoFactory.CreatePrivateKeyInfo(
                    password, wrongPkcs12Zero, encPrivKeyInfo);

                LoadKeyBag(privInfo, bagAttributes);
            }
        }

        private void LoadKeyBag(PrivateKeyInfo privKeyInfo, Asn1Set bagAttributes)
        {
            AsymmetricKeyParameter privKey = PrivateKeyFactory.CreateKey(privKeyInfo);

            IDictionary attributes = new Hashtable();
            AsymmetricKeyEntry keyEntry = new AsymmetricKeyEntry(privKey, attributes);

            string alias = null;
            Asn1OctetString localId = null;

            if (bagAttributes != null)
            {
                foreach (Asn1Sequence sq in bagAttributes)
                {
                    DerObjectIdentifier aOid = DerObjectIdentifier.GetInstance(sq[0]);
                    Asn1Set attrSet = Asn1Set.GetInstance(sq[1]);
                    Asn1Encodable attr = null;

                    if (attrSet.Count > 0)
                    {
                        // TODO We should be adding all attributes in the set
                        attr = attrSet[0];

                        // TODO We might want to "merge" attribute sets with
                        // the same OID - currently, differing values give an error
                        if (attributes.Contains(aOid.Id))
                        {
                            // OK, but the value has to be the same
                            if (!attributes[aOid.Id].Equals(attr))
                                throw new IOException("attempt to add existing attribute with different value");
                        }
                        else
                        {
                            attributes.Add(aOid.Id, attr);
                        }

                        if (aOid.Equals(PkcsObjectIdentifiers.Pkcs9AtFriendlyName))
                        {
                            alias = ((DerBmpString)attr).GetString();
                            // TODO Do these in a separate loop, just collect aliases here
                            keys[alias] = keyEntry;
                        }
                        else if (aOid.Equals(PkcsObjectIdentifiers.Pkcs9AtLocalKeyID))
                        {
                            localId = (Asn1OctetString)attr;
                        }
                    }
                }
            }

            if (localId != null)
            {
                string name = Hex.ToHexString(localId.GetOctets());

                if (alias == null)
                {
                    keys[name] = keyEntry;
                }
                else
                {
                    // TODO There may have been more than one alias
                    localIds[alias] = name;
                }
            }
            else
            {
                unmarkedKeyEntry = keyEntry;
            }
        }

        internal class CertId
        {
            private readonly byte[] id;

            internal CertId(AsymmetricKeyParameter pubKey)
            {
                this.id = PkcsStore.CreateSubjectKeyID(pubKey).GetKeyIdentifier();
            }

            internal CertId(
                byte[] id)
            {
                this.id = id;
            }

            internal byte[] Id
            {
                get { return id; }
            }

            public override int GetHashCode()
            {
                return Arrays.GetHashCode(id);
            }

            public override bool Equals(
                object obj)
            {
                if (obj == this)
                    return true;

                CertId other = obj as CertId;

                if (other == null)
                    return false;

                return Arrays.AreEqual(id, other.id);
            }
        }
    }
}
