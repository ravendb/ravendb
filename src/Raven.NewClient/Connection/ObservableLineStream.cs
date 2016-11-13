// -----------------------------------------------------------------------
//  <copyright file="EventSourceStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Extensions;

using Sparrow.Collections;
using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Connection
{
    public class ObservableLineStream : IObservable<string>, IDisposable
    {
        private readonly ConcurrentSet<IObserver<string>> subscribers = new ConcurrentSet<IObserver<string>>();
        private readonly Stream stream;
        private readonly byte[] buffer = new byte[8192];
        private readonly Action onDispose;
        private readonly object taskFaultedSyncObj = new object();

        private int posInBuffer;
        private bool onDisposeCalled;

        public ObservableLineStream(Stream stream, Action onDispose)
        {
            this.stream = stream;
            this.onDispose = onDispose;
        }

        public void Start()
        {
            ReadAsync()
                .ContinueWith(task =>
                                {
                                    var read = task.Result;
                                    if (read == 0)// will force reopening of the connection
                                        throw new EndOfStreamException();
                                    // find \r\n in newly read range

                                    var startPos = 0;
                                    byte prev = 0;
                                    bool foundLines = false;
                                    for (int i = posInBuffer; i < posInBuffer + read; i++)
                                    {
                                        if (prev == '\r' && buffer[i] == '\n')
                                        {
                                            foundLines = true;
                                            var oldStartPos = startPos;
                                            // yeah, we found a line, let us give it to the users
                                            startPos = i + 1;

                                            // is it an empty line?
                                            if (oldStartPos == i - 2)
                                            {
                                                continue; // ignore and continue
                                            }

                                            // first 5 bytes should be: 'd','a','t','a',':'
                                            // if it isn't, ignore and continue
                                            if (buffer.Length - oldStartPos < 5 ||
                                                buffer[oldStartPos] != 'd' ||
                                                buffer[oldStartPos + 1] != 'a' ||
                                                buffer[oldStartPos + 2] != 't' ||
                                                buffer[oldStartPos + 3] != 'a' ||
                                                buffer[oldStartPos + 4] != ':')
                                            {
                                                continue;
                                            }

                                            var data = Encoding.UTF8.GetString(buffer, oldStartPos + 5, i - oldStartPos - 6);
                                            foreach (var subscriber in subscribers)
                                            {
                                                subscriber.OnNext(data);
                                            }
                                        }
                                        prev = buffer[i];
                                    }
                                    posInBuffer += read;
                                    if (startPos >= posInBuffer) // read to end
                                    {
                                        posInBuffer = 0;
                                        return;
                                    }
                                    if (foundLines == false)
                                        return;

                                    // move remaining to the start of buffer, then reset
                                    Array.Copy(buffer, startPos, buffer, 0, posInBuffer - startPos);
                                    posInBuffer -= startPos;
                                })
                                .ContinueWith(task =>
                                {
                                    if (task.IsFaulted)
                                    {
                                        DisposeAndSingalConnectionError(task);
                                        return;
                                    }

                                    Start(); // read more lines
                                });
        }

        private void DisposeAndSingalConnectionError(Task task)
        {
            if (Monitor.TryEnter(taskFaultedSyncObj) == false)
                return;

            try
            {
                try
                {
                    stream.Dispose();
                }
                catch (Exception)
                {
                    // explicitly ignoring this
                }

                //make sure the existing connection is returned
                //to http client cache
                //since the stream has faulted, we will either
                //reconnect or fail the Changes API with exception
                //so in any case we need to cleanup things

                if (onDisposeCalled == false)
                {
                    onDispose();
                    onDisposeCalled = true;
                }

                var aggregateException = task.Exception;
                var exception = aggregateException.ExtractSingleInnerException();
                if (exception is ObjectDisposedException)
                    return;

                var we = exception as WebException;
                if (we != null && we.Status == WebExceptionStatus.RequestCanceled)
                    return;

                foreach (var subscriber in subscribers)
                {
                    subscriber.OnError(aggregateException);
                }
            }
            finally
            {
                Monitor.Exit(taskFaultedSyncObj);
            }
        }

        private async Task<int> ReadAsync()
        {
            return await stream
                .ReadAsync(buffer, posInBuffer, buffer.Length - posInBuffer)
                .ConfigureAwait(false);
        }

        public IDisposable Subscribe(IObserver<string> observer)
        {
            subscribers.TryAdd(observer);
            return new DisposableAction(() => subscribers.TryRemove(observer));
        }

        public void Dispose()
        {
            foreach (var subscriber in subscribers)
            {
                subscriber.OnCompleted();
            }

            if (onDisposeCalled)
                return;

            lock (taskFaultedSyncObj)
            {
                if (onDisposeCalled)
                    return;

                onDispose();
                onDisposeCalled = true;
            }
        }
    }
}
