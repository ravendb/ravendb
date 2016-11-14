namespace Raven.NewClient.Abstractions.Util.Encryptors
{
    using System;
    using System.Security.Cryptography;

    public abstract class HashEncryptorBase
    {
        protected bool AllowNonThreadSafeMethods { get; private set; }

        protected HashEncryptorBase(bool allowNonThreadSafeMethods)
        {
            AllowNonThreadSafeMethods = allowNonThreadSafeMethods;
        }

        protected byte[] ComputeHashInternal(HashAlgorithm algorithm, byte[] bytes, int? size = null)
        {
            var hash = algorithm.ComputeHash(bytes);
            if (size.HasValue)
                Array.Resize(ref hash, size.Value);

            return hash;
        }

        protected byte[] ComputeHashInternal(HashAlgorithm algorithm, byte[] bytes, int offset, int count, int? size = null)
        {
            var hash = algorithm.ComputeHash(bytes, offset, count);
            if (size.HasValue)
                Array.Resize(ref hash, size.Value);

            return hash;
        }

        public byte[] ComputeHash(HashAlgorithm algorithm, byte[] bytes, int? size = null)
        {
            using (algorithm)
            {
                var hash = algorithm.ComputeHash(bytes);
                if (size.HasValue)
                    Array.Resize(ref hash, size.Value);

                return hash;
            }
        }

        public byte[] ComputeHash(HashAlgorithm algorithm, byte[] bytes, int offset, int count, int? size = null)
        {
            using (algorithm)
            {
                var hash = algorithm.ComputeHash(bytes, offset, count);
                if (size.HasValue)
                    Array.Resize(ref hash, size.Value);

                return hash;
            }
        }

        protected void ThrowNotSupportedExceptionForNonThreadSafeMethod()
        {
            if (AllowNonThreadSafeMethods == false)
                throw new NotSupportedException("Non-thread-safe methods are not allowed.");
        }
    }
}
