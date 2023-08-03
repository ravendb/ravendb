using System.Collections.Generic;
using System.IO;
using Confluent.Kafka;
using Org.BouncyCastle.Utilities.IO.Pem;
using Raven.Client.Documents.Operations.QueueSink;
using PemWriter = Org.BouncyCastle.OpenSsl.PemWriter;

namespace Raven.Server.Documents.QueueSink;

public class KafkaQueueSink : QueueSinkProcess
{
    public KafkaQueueSink(QueueSinkConfiguration configuration, QueueSinkScript script, DocumentDatabase database, string tag) : base(configuration, script, database, tag)
    {
    }

    protected override IQueueSinkConsumer CreateConsumer()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = Configuration.Connection.KafkaConnectionSettings.BootstrapServers,
            GroupId = GroupId,
            IsolationLevel = IsolationLevel.ReadCommitted,
            // we are disabling auto commit option and we are manually commit only messages that are processed successfully
            EnableAutoCommit = false,
            // we are using Earliest option because we want to be able to see messages which are present before consumer is connected
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        var settings = Configuration.Connection.KafkaConnectionSettings;
        var certificateHolder = Database.ServerStore.Server.Certificate;
        
        if (settings.UseRavenCertificate && certificateHolder?.Certificate != null)
        {
            consumerConfig.SslCertificatePem = ExportAsPem(new PemObject("CERTIFICATE", certificateHolder.Certificate.RawData));
            consumerConfig.SslKeyPem = ExportAsPem(certificateHolder.PrivateKey.Key);
            consumerConfig.SecurityProtocol = SecurityProtocol.Ssl;
        }

        if (settings.ConnectionOptions != null)
        {
            foreach (KeyValuePair<string, string> option in settings.ConnectionOptions)
            {
                consumerConfig.Set(option.Key, option.Value);
            }
        }

        var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(Script.Queues);

        return new KafkaSinkConsumer(consumer);
    }
    
    private static string ExportAsPem(object @object)
    {
        using (var sw = new StringWriter())
        {
            var pemWriter = new PemWriter(sw);
            
            pemWriter.WriteObject(@object);

            return sw.ToString();
        }
    }
}
