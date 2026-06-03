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
    private readonly ServiceBusClient _client;
    private readonly RavenLogger _logger;
    private readonly CancellationToken _token;
    private readonly List<Task> _receiveTasks = new();

    private readonly Channel<IMessage> _deliveries =
        Channel.CreateBounded<IMessage>(new BoundedChannelOptions(1024)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true
        });
    private readonly ConcurrentBag<IMessage> _pendingCompletions = new();
    private readonly CancellationTokenSource _cts;

    public AzureServiceBusSinkConsumer(ServiceBusClient client, RavenLogger logger, CancellationToken token)
    {
        _client = client;
        _logger = logger;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
        _token = _cts.Token;
    }

    public void SubscribeToQueue(string queue)
    {
        var receiver = _client.CreateReceiver(queue, CreateReceiverOptions());
        _receiveTasks.Add(Run(receiver));
    }

    public void SubscribeToSubscription(string topic, string subscription)
    {
        var receiver = _client.CreateReceiver(topic, subscription, CreateReceiverOptions());
        _receiveTasks.Add(Run(receiver));
    }

    private static ServiceBusReceiverOptions CreateReceiverOptions() => new()
    {
        // The default renewal time is 30s if more time is needed, has to be configured on the entity itself (max is 5 min).
        ReceiveMode = ServiceBusReceiveMode.PeekLock,
        PrefetchCount = 32
    };

    private async Task Run(ServiceBusReceiver receiver)
    {
        while (_token.IsCancellationRequested == false)
        {
            try
            {
                var message = await receiver.ReceiveMessageAsync(cancellationToken: _token);
                if (message == null)
                    continue;

                await _deliveries.Writer.WriteAsync(new MessageContext(receiver, message), _token);
            }
            catch (Exception e) when (e is ChannelClosedException or OperationCanceledException)
            {
                // shutdown requested, exit the loop
                break;
            }
            catch (Exception e)
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Error while processing messages from Azure Service Bus receiver for '{receiver.EntityPath}'", e);

                try
                {
                    await _deliveries.Writer.WriteAsync(new ExceptionMessage(e), _token);
                    await Task.Delay(TimeSpan.FromSeconds(15), _token);
                }
                catch
                {
                    // nothing we can do
                }
            }
        }

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
            await receiver.CloseAsync(cts.Token); // same as dispose but with token
        }
        catch (Exception e)
        {
            if (_logger.IsErrorEnabled)
                _logger.Error($"Error while disposing Azure Service Bus receiver for '{receiver.EntityPath}'", e);
        }
    }

    public byte[] Consume(CancellationToken cancellationToken)
    {
        try
        {
            var ctx = _deliveries.Reader.ReadAsync(cancellationToken).AsTask().GetAwaiter().GetResult();
            return ctx.Consume(_pendingCompletions);
        }
        catch (Exception e) when (e is ChannelClosedException or OperationCanceledException)
        {
            return null;
        }
    }

    public byte[] Consume(TimeSpan timeout)
    {
        if (timeout == TimeSpan.Zero)
        {
            if (_deliveries.Reader.TryRead(out var ctx) == false)
                return null;

            return ctx.Consume(_pendingCompletions);
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        cts.CancelAfter(timeout);
        return Consume(cts.Token);
    }

    public void Commit()
    {
        if (_pendingCompletions.IsEmpty)
            return;

        Parallel.ForEachAsync(_pendingCompletions, _token, async (ctx, token) =>
        {
            try
            {
                await ctx.Receiver.CompleteMessageAsync(ctx.Message, token);
            }
            catch (Exception e)
            {
                // The broker will redeliver the message once its lock expires. Most common cause is
                // a batch that took longer than the entity's LockDuration - configure it to its max (5 min).
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Failed to complete Azure Service Bus message '{ctx.Message.MessageId}' on '{ctx.Receiver.EntityPath}'", e);
            }
        }).GetAwaiter().GetResult();

        _pendingCompletions.Clear();
    }

    public void Dispose()
    {
        using (_cts)
        {
            _cts.Cancel();
            _deliveries.Writer.TryComplete();

            Task.WaitAll(_receiveTasks.ToArray(), TimeSpan.FromMinutes(1));
            try
            {
                _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch
            {
                // nothing we can do
            }
        }
    }

    private interface IMessage
    {
        public ServiceBusReceivedMessage Message { get; }

        public ServiceBusReceiver Receiver => throw new NotImplementedException();

        public byte[] Consume(ConcurrentBag<IMessage> pending)
        {
            var message = Message.Body.ToArray();
            pending.Add(this);
            return message;
        }
    }

    private sealed record ExceptionMessage(Exception Exception) : IMessage
    {
        public ServiceBusReceivedMessage Message
        {
            get
            {
                ExceptionDispatchInfo.Throw(Exception);
                return null; // Unreachable, but required to satisfy the compiler.
            }
        }
    }

    private sealed record MessageContext(ServiceBusReceiver Receiver, ServiceBusReceivedMessage Message) : IMessage;
}
