using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Bundles.Encryption.Settings;
using Raven.Bundles.Encryption.Streams;

namespace Raven.Bundles.Encryption.IndexFileCodec
{
	class Program
	{
		private enum ExitCodes
		{
			InvalidArguments = 3,
			EncryptionError = 1
		}

		private static readonly Dictionary<string, Action<byte[], string, Stream, Stream>> methods = new Dictionary<string, Action<byte[], string, Stream, Stream>> {
			{ "encrypt", Encrypt },
			{ "decrypt", Decrypt }
		};

		static void Main(string[] args)
		{
			if (args.Length != 5 || !methods.ContainsKey(args[0]))
			{
				Console.Error.WriteLine(@"Raven index file codec

Usage: Raven.Bundles.Encryption.IndexFileCodec {method} {key} {filename} {input} {output}

This tool only uses standard input and output for IO.
Method may be encrypt or decrypt.
The key must be a base64 encryption key.
The filename must be the name of the file (without path) where it was originally used, as that is part of the encryption.

This tool will read the file contents from the specified input file, encrypt or decrypt them, and write to the specified output file.
If decryption fails, a message will be written to standard error.
");
				Environment.Exit((int)ExitCodes.InvalidArguments);
				return;
			}

			byte[] password;
			try
			{
				password = Convert.FromBase64String(args[1]);
			}
			catch (Exception)
			{
				Console.Error.WriteLine("Invalid base64 encoding for encryption password.");
				Environment.Exit((int)ExitCodes.InvalidArguments);
				return;
			}

			try
			{
				using (var input = File.Open(args[3], FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
				using (var output = File.Open(args[4], FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite))
					methods[args[0]](password, args[2], input, output);
			}
			catch (Exception ex)
			{
				Console.Error.WriteLine(ex);
				Environment.Exit((int)ExitCodes.EncryptionError);
			}
		}

		static void Encrypt(byte[] password, string file, Stream input, Stream output)
		{
			input.CopyTo(new SeekableCryptoStream(new EncryptionSettings(password), file, output));
		}

		static void Decrypt(byte[] password, string file, Stream input, Stream output)
		{
			new SeekableCryptoStream(new EncryptionSettings(password), file, input).CopyTo(output);
		}
	}
}
