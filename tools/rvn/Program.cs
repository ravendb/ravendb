using System;
using System.IO;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Voron;
using Voron.Impl.Compaction;

namespace rvn
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                PrintUsage($"Expected 3 arguments but got {args.Length}");
                return;
            }

            var command = args[1].ToLowerInvariant();
            var path = args[2];

            ValidatePath(path);
            
            switch (command)
            {
                case "encrypt":
                    EncryptServerStore(path);
                    break;
                case "get-key":
                    var base64Key = RecoverServerStoreKey(path);
                    Console.WriteLine("Secret Key:");
                    Console.WriteLine(base64Key);
                    break;
                case "put-key":
                    Console.WriteLine("Please enter the secret key: ");
                    var key = Console.ReadLine();
                    ValidateKey(key);
                    InstallServerStoreKey(path, key);
                    break;
                case "decrypt":
                    DecryptServerStore(path);
                    break;
                default:
                    PrintUsage("Invalid command...");
                    return;
            }

            Console.WriteLine("Done.");
        }

        private static void ValidatePath(string dirPath)
        {
            if (dirPath == null)
                throw new ArgumentNullException(nameof(dirPath));
            if (!Directory.Exists(dirPath))
                throw new DirectoryNotFoundException(dirPath);
        }

        private static void ValidateKey(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
        }

        private static void PrintUsage(string error = null)
        {
            Console.WriteLine(error ?? "");
            Console.WriteLine(@"
Server Store Encryption & Backup utility for RavenDB
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------", DateTime.UtcNow.Year);

            Console.WriteLine(@"
Description: 
    This utility lets you manage RavenDB offline operations, such as setting encryption mode for the server store.

    The server store which may contains sensitive information is not encrypted by default (even if it contains encrypted databases).
    If you want it encrypted, you must do it manually using this tool.
    
Usage:

Setup encryption for the server store or decrypt an encrypted store. All commands MUST run under the same user as the one that
the RavenDB server is using.
The server MUST be offline for the duration of those operations.


    rvn server encrypt <path>

        The encrypt command gets the path of RavenDB's system directory, encrypts the files and saves the key to the same directory.
        This key file (secret.key.encrypted) is protected for the current OS user. Once encrypted, The server will only work for 
        the current user.
        It is recommended that you'll do that as part of the initial setup of the server, before it is running. Encrypted server 
        store can only talk to other encrypted server stores, and only over SSL. 

    rvn server decrypt <path>

        The decrypt command gets the path of RavenDB's system directory on the new machine. It will decrypt the files in that 
        directory using the key which was inserted earlier using the put-key command.


In order to backup the files (for any user) and possibly transfer them to a different machine use the following:

    rvn server get-key <path>

        Once the server store is encrypted run the get-key command with the system directory path, the output it the unprotected key.
        This key will allow decryption of the server store and must be kept safely. This is REQUIRED when restoring backups from an 
        encrypted server store.


    rvn server put-key <path>

        To restore, on a new machine, run the put-key command with the system directory path. This will protect the key on the new machine
        for the current OS user. This is typically used as part of the restore process of an encrypted server store on a new machine.

");
        }

        private static void EncryptServerStore(string srcDir)
        {
            var masterKey = Sodium.GenerateMasterKey();
            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Encryption");

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            dstOptions.MasterKey = masterKey;

            var entropy = Sodium.GenerateRandomBuffer(256);
            var protect = SecretProtection.Protect(masterKey, entropy);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            using (var f = File.OpenWrite(Path.Combine(dstDir, "secret.key.encrypted")))
            {
                f.Write(protect, 0, protect.Length);
                f.Write(entropy, 0, entropy.Length);
                f.Flush();
            }

            IOExtensions.DeleteDirectory(srcDir);
            Directory.Move(dstDir, srcDir);
        }

        private static string RecoverServerStoreKey(string srcDir)
        {
            var keyPath = Path.Combine(srcDir, "secret.key.encrypted");
            if (File.Exists(keyPath) == false)
                return null;

            var buffer = File.ReadAllBytes(keyPath);
            var secret = new byte[buffer.Length - 32];
            var entropy = new byte[32];
            Array.Copy(buffer, 0, secret, 0, buffer.Length - 32);
            Array.Copy(buffer, buffer.Length - 32, entropy, 0, 32);

            var key = SecretProtection.Unprotect(secret, entropy);
            return Convert.ToBase64String(key);
        }

        private static void InstallServerStoreKey(string dstDir, string base64Key)
        {
            var entropy = Sodium.GenerateRandomBuffer(256);
            var secret = Convert.FromBase64String(base64Key);
            var protect = SecretProtection.Protect(secret, entropy);

            using (var f = File.OpenWrite(Path.Combine(dstDir, "secret.key.encrypted")))
            {
                f.Write(protect, 0, protect.Length);
                f.Write(entropy, 0, entropy.Length);
                f.Flush();
            }
        }

        private static void DecryptServerStore(string srcDir)
        {
            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Decryption");

            var bytes = File.ReadAllBytes(Path.Combine(srcDir, "secret.key.encrypted"));
            var secret = new byte[bytes.Length - 32];
            var entropy = new byte[32];
            Array.Copy(bytes, 0, secret, 0, bytes.Length - 32);
            Array.Copy(bytes, bytes.Length - 32, entropy, 0, 32);

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            srcOptions.MasterKey = SecretProtection.Unprotect(secret, entropy);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            IOExtensions.DeleteDirectory(srcDir);
            Directory.Move(dstDir, srcDir);
        }


    }
}