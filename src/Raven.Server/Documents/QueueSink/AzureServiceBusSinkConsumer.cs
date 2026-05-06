using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.QueueSink;

public class AzureServiceBusSinkConsumer : IQueueSinkConsumer
{
    private const int ChannelCapacity = 1024;

    private readonly ServiceBusClient _client;
    private readonly RavenLogger _logger;
    private readonly CancellationToken _cancellationToken;
    private readonly List<ServiceBusProcessor> _processors = new();

    private readonly Channel<MessageContext> _deliveries =
        Channel.CreateBounded<MessageContext>(new BoundedChannelOptions(ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true
        });
    private readonly ConcurrentQueue<MessageContext> _pendingCompletions = new();

    public AzureServiceBusSinkConsumer(ServiceBusClient client, RavenLogger logger, CancellationToken cancellationToken)
    {
        _client = client;
        _logger = logger;
        _cancellationToken = cancellationToken;
    }

    public void SubscribeToQueue(string queue) => _processors.Add(_client.CreateProcessor(queue, CreateProcessorOptions()));

    public void SubscribeToSubscription(string topic, string subscription) => _processors.Add(_client.CreateProcessor(topic, subscription, CreateProcessorOptions()));

    private static ServiceBusProcessorOptions CreateProcessorOptions() => new()
    {
        ReceiveMode = ServiceBusReceiveMode.PeekLock,
        AutoCompleteMessages = false,
        MaxConcurrentCalls = 8,
        MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5)
    };

    public async Task StartAsync()
    {
        foreach (var processor in _processors)
        {
            processor.ProcessMessageAsync += OnMessageAsync;
            processor.ProcessErrorAsync += OnErrorAsync;
            await processor.StartProcessingAsync(_cancellationToken);
        }
    }

    private async Task OnMessageAsync(ProcessMessageEventArgs args)
    {
        var ctx = new MessageContext(args);
        await _deliveries.Writer.WriteAsync(ctx, args.CancellationToken);
        await ctx.Gate.Task;
    }

    private async Task OnErrorAsync(ProcessErrorEventArgs args)
    {
        if (_logger.IsErrorEnabled)
            _logger.Error($"Azure Service Bus processor error for '{args.EntityPath}' (source: {args.ErrorSource})", args.Exception);

        try
        {
            await _deliveries.Writer.WriteAsync(new MessageContext(args.Exception), _cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // shutting down, ignore
        }
    }

    public byte[] Consume(CancellationToken cancellationToken)
    {
        // Called only on a new batch start.
        EnsureNoPendingRequests();

        var ctx = _deliveries.Reader.ReadAsync(cancellationToken).AsTask().GetAwaiter().GetResult();
        ctx.EnsureNoException();

        _pendingCompletions.Enqueue(ctx);
        return ctx.Args.Message.Body.ToArray();
    }

    public byte[] Consume(TimeSpan timeout)
    {
        if (timeout == TimeSpan.Zero)
        {
            if (_deliveries.Reader.TryRead(out var ctx) == false)
                return null;

            ctx.EnsureNoException();
            
            _pendingCompletions.Enqueue(ctx);
            return ctx.Args.Message.Body.ToArray();
        }

        using var cts = new CancellationTokenSource(timeout);
        try
        {
            var ctx = _deliveries.Reader.ReadAsync(cts.Token).AsTask().GetAwaiter().GetResult();
            ctx.EnsureNoException();
            
            _pendingCompletions.Enqueue(ctx);
            return ctx.Args.Message.Body.ToArray();
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    public void Commit()
    {
        if (_pendingCompletions.IsEmpty)
            return;

        var batch = new List<MessageContext>();
        while (_pendingCompletions.TryDequeue(out var ctx))
            batch.Add(ctx);

        Parallel.ForEachAsync(batch, _cancellationToken, async (ctx, token) =>
        {
            try
            {
                await ctx.Args.CompleteMessageAsync(ctx.Args.Message, token);
            }
            finally
            {
                ctx.Gate.TrySetResult();
            }
        }).GetAwaiter().GetResult();
    }

    private void EnsureNoPendingRequests()
    {
        // Release any pending gates so parked handlers can return
        while (_pendingCompletions.TryDequeue(out var ctx))
        {
            ctx.Gate.TrySetResult();
        }
    }

    public void Dispose()
    {
        _deliveries.Writer.TryComplete();

        EnsureNoPendingRequests();

        // Drain any contexts still sitting in the channel
        while (_deliveries.Reader.TryRead(out var ctx))
        {
            ctx.Gate.TrySetResult();
        }

        // Bounded timeout so a hung Service Bus SDK can't block database/server shutdown.
        using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        Parallel.ForEachAsync(_processors, async (processor, _) =>
        {
            try
            {
                await using (processor)
                {
                    await processor.StopProcessingAsync(stopCts.Token);
                }
            }
            catch (Exception ex)
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Error while disposing Azure Service Bus processor for '{processor.EntityPath}'", ex);
            }
        }).GetAwaiter().GetResult();

        _processors.Clear();
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private sealed class MessageContext
    {
        private Exception Exception { get; }

        public ProcessMessageEventArgs Args { get; }

        public TaskCompletionSource Gate { get; }

        public MessageContext(Exception exception)
        {
            Exception = exception;
        }

        public MessageContext(ProcessMessageEventArgs args)
        {
            Args = args;
            Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void EnsureNoException()
        {
            if (Exception == null)
                return;

            ExceptionDispatchInfo.Capture(Exception).Throw();
        }
    }
}
