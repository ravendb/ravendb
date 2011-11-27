namespace Rhino.Licensing
{
    /// <summary>
    /// Validates content of a license file
    /// </summary>
    public class StringLicenseValidator : AbstractLicenseValidator
    {
        /// <summary>
        /// Creates a new instance of <seealso cref="StringLicenseValidator"/>
        /// </summary>
        /// <param name="publicKey">public key</param>
        /// <param name="license">license content</param>
        public StringLicenseValidator(string publicKey, string license)
            : base(publicKey)
        {
            License = license;
        }

        /// <summary>
        /// License content
        /// </summary>
        protected override string License
        {
            get; set;
        }
    }
}