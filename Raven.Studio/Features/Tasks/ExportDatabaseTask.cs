using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Smuggler;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Path = System.IO.Path;
using TaskStatus = System.Threading.Tasks.TaskStatus;

namespace Raven.Studio.Features.Tasks
{
    public class ExportDatabaseTask : DatabaseTask
    {
        const int BatchSize = 512;

        private bool includeAttachements, includeDocuments, includeIndexes;
        private readonly bool removeAnalyzers;
        private readonly bool includeTransformers;
        private readonly bool shouldExcludeExpired;
        private readonly int batchSize;
        private readonly string transformScript;
        private readonly List<FilterSetting> filterSettings;

        public ExportDatabaseTask(IAsyncDatabaseCommands databaseCommands, string databaseName, bool includeAttachements, bool includeDocuments, bool includeIndexes,
                bool removeAnalyzers, bool includeTransformers, bool shouldExcludeExpired, int batchSize, string transformScript, List<FilterSetting> filterSettings)
                    : base(databaseCommands, databaseName, "Export Database")
        {
            this.includeAttachements = includeAttachements;
            this.includeDocuments = includeDocuments;
            this.includeIndexes = includeIndexes;
            this.removeAnalyzers = removeAnalyzers;
            this.includeTransformers = includeTransformers;
            this.shouldExcludeExpired = shouldExcludeExpired;
            this.batchSize = batchSize;
            this.transformScript = transformScript;
            this.filterSettings = filterSettings;
        }

        protected override async Task<DatabaseTaskOutcome> RunImplementation()
        {
            if (includeDocuments == false && includeAttachements == false && includeIndexes == false && includeTransformers == false)
                return DatabaseTaskOutcome.Abandoned;

            var saveFile = new SaveFileDialog
            {
                DefaultExt = ".ravendump",
                Filter = "Raven Dumps|*.ravendump;*.raven.dump",
            };

            var name = ApplicationModel.Database.Value.Name;
            var normalizedName = new string(name.Select(ch => Path.GetInvalidPathChars().Contains(ch) ? '_' : ch).ToArray());
            var defaultFileName = string.Format("Dump of {0}, {1}", normalizedName, DateTimeOffset.Now.ToString("dd MMM yyyy HH-mm", CultureInfo.InvariantCulture));
            try
            {
                saveFile.DefaultFileName = defaultFileName;
            }
            catch { }

            if (saveFile.ShowDialog() != true)
            {
                return DatabaseTaskOutcome.Abandoned;
            }

            using (var stream = saveFile.OpenFile())
            {
                ItemType operateOnTypes = 0;

                if (includeDocuments)
                {
                    operateOnTypes |= ItemType.Documents;
                }

                if (includeAttachements)
                {
                    operateOnTypes |= ItemType.Attachments;
                }

                if (includeIndexes)
                {
                    operateOnTypes |= ItemType.Indexes;
                }

                if (removeAnalyzers)
                {
                    operateOnTypes |= ItemType.RemoveAnalyzers;
                }

                if (includeTransformers)
                {
                    operateOnTypes |= ItemType.Transformers;
                }

                var smuggler = new SmugglerApi(DatabaseCommands, message => Report(message));

                var forwardtoUiBoundStream = new ForwardtoUIBoundStream(stream);
	            var taskGeneration = new Task<Task>(() => smuggler.ExportData(new SmugglerExportOptions {ToStream = forwardtoUiBoundStream}, new SmugglerOptions
	            {
		            BatchSize = batchSize,
		            Filters = filterSettings,
		            TransformScript = transformScript,
		            ShouldExcludeExpired = shouldExcludeExpired,
		            OperateOnTypes = operateOnTypes,
		            Incremental = false,
	            }));

                ThreadPool.QueueUserWorkItem(state => taskGeneration.Start());


                await taskGeneration.Unwrap();
                forwardtoUiBoundStream.Flush();
                stream.Flush();
            }

            return DatabaseTaskOutcome.Succesful;
        }

        public class ForwardtoUIBoundStream : Stream
        {
            readonly byte[] localBuffer = new byte[4 * 1024 * 1024];
            private int pos;

            private readonly Stream inner;

            public ForwardtoUIBoundStream(Stream inner)
            {
                this.inner = inner;
            }

            public override void Flush()
            {
                Execute.OnTheUI(() =>
                {
                    inner.Write(localBuffer, 0, pos);
                    pos = 0;
                    inner.Flush();
                }).Wait();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                while (count > 0)
                {
                    var bytes = Math.Min(localBuffer.Length - pos, count);
                    Buffer.BlockCopy(buffer, offset, localBuffer, pos, bytes);
                    pos += bytes;
                    offset += bytes;
                    count -= bytes;
                    if (pos == localBuffer.Length)
                        Flush();
                }
            }

            public override bool CanRead
            {
                get { return false; }
            }
            public override bool CanSeek
            {
                get { return false; }
            }
            public override bool CanWrite
            {
                get { return true; }
            }
            public override long Length
            {
                get { throw new NotSupportedException(); }
            }
            public override long Position
            {
                get { throw new NotSupportedException(); }
                set
                {
                    throw new NotSupportedException();
                }
            }
        }

        public override void OnError()
        {

        }
    }
}
