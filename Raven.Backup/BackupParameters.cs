using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using System;
using System.Net;
namespace Raven.Backup
{
    public class BackupParameters
    {
        public string ServerUrl { get; set; }
        public string BackupPath { get; set; }
        public bool NoWait { get; set; }
        public NetworkCredential Credentials { get; set; }

        public bool Incremental { get; set; }
        public int? Timeout { get; set; }
        public string ApiKey { get; set; }
        public string Database { get; set; }
        public string Filesystem { get; set; }

        public BackupParameters()
        {
            Credentials = CredentialCache.DefaultNetworkCredentials;
        }

    }
}