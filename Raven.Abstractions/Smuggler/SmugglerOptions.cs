using System;
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler
{
    public abstract class SmugglerOptions
    {
        public CancellationTokenSource CancelToken;

        public const int DefaultDocumentSizeInChunkLimitInBytes = 32 * 1024 * 1024;
        public abstract string SourceUrl { get; }
        public abstract string DestinationUrl { get; }

        public bool Incremental { get; set; }
        public string BackupPath { get; set; }

        private TimeSpan timeout;
        /// <summary>
        /// The timeout for requests
        /// </summary>
        public TimeSpan Timeout
        {
            get { return timeout; }
            set
            {
                if (value < TimeSpan.FromSeconds(5))
                {
                    throw new InvalidOperationException("Timeout value cannot be less then 5 seconds.");
                }
                timeout = value;
            }
        }
        protected SmugglerOptions()
        {
            CancelToken = new CancellationTokenSource();
        }
    }

    public class SmugglerOptions<T> : SmugglerOptions where T : ConnectionStringOptions, new()
    {
        private int batchSize;

        public T Source { get; set; }
        public T Destination { get; set; }

        public override string SourceUrl { get { return Source.Url; } }
        public override string DestinationUrl { get { return Destination.Url; } }

        public int Limit { get; set; }


        /// <summary>
        /// The number of entities to load in each call to the server.
        /// </summary>
        public int BatchSize
        {
            get { return batchSize; }
            set
            {
                if (value < 1)
                    throw new InvalidOperationException("Batch size cannot be zero or a negative number");
                batchSize = value;
            }
        }

        public SmugglerOptions()
        {
            CancelToken = new CancellationTokenSource();
            Limit = int.MaxValue;
            BatchSize = 16 * 1024;
            Source = new T();
            Destination = new T();
        }
    }

    public class SmugglerBetweenOptions<T> where T : ConnectionStringOptions
    {
        public T From { get; set; }

        public T To { get; set; }

        /// <summary>
        /// You can give a key to the incremental last etag, in order to make incremental imports from a few export sources.
        /// </summary>
        public string IncrementalKey { get; set; }

		public Action<string> ReportProgress { get; set; } 
    }

    public class SmugglerExportOptions<T> where T : ConnectionStringOptions
    {
        public T From { get; set; }

        /// <summary>
        /// The path to write the export.
        /// </summary>
        public string ToFile { get; set; }

        /// <summary>
        /// The stream to write the export.
        /// </summary>
        public Stream ToStream { get; set; }
    }

    public class SmugglerImportOptions<T> where T : ConnectionStringOptions
    {
        public T To { get; set; }

        /// <summary>
        /// The path to read from of the import data.
        /// </summary>
        public string FromFile { get; set; }

        /// <summary>
        /// The stream to read from of the import data.
        /// </summary>
        public Stream FromStream { get; set; }
    }
}