using System;
using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;

namespace Raven.Server.Documents.AI;

public class AiTasksLoader : IDisposable
{
//    public void HandleDatabaseRecordChange(DatabaseRecord record)
//    {
//        if (record == null)
//            return;

//        foreach (var connectionStringKvp in record.AiConnectionStrings)
//        {
//            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionStringKvp.Value.Identifier);
//            var connectionString = connectionStringKvp.Value;

//            if (_servicesByConnectionStringIdentifier.ContainsKey(connectionStringIdentifier))
//                continue;

//            var kernelBuilder = Kernel.CreateBuilder();
//            kernelBuilder.Configure(connectionString, isConnectionTest: false);
//            var kernel = kernelBuilder.Build();
//#pragma warning disable SKEXP0001
//            var service = kernel.GetRequiredService<ITextEmbeddingGenerationService>();
//#pragma warning restore SKEXP0001

//            _servicesByConnectionStringIdentifier[connectionStringIdentifier] = service;
//        }

//        // todo skip disabled tasks?
//        foreach (var aiIntegrationConfiguration in record.AiIntegrations)
//        {
//            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(aiIntegrationConfiguration.Identifier);
//            var connectionStringIdentifier = new AiConnectionStringIdentifier(record.AiConnectionStrings[aiIntegrationConfiguration.ConnectionStringName].Identifier);

//            var service = _servicesByConnectionStringIdentifier[connectionStringIdentifier];

//            _servicesByIntegrationTaskIdentifier[aiIntegrationIdentifier] = service;

//            _taskIdentifierToConnectionStringIdentifier[aiIntegrationIdentifier] = connectionStringIdentifier;
//        }

//        /*
//        if (_embeddingsCacher.IsStarted)
//        {
//            if (record.AiIntegrations.Count == 0)
//            {
//                _embeddingsCacher.Stop();
//                _embeddingsCacher.IsStarted = false;
//            }
//            return;
//        }
//        */

//        //_embeddingsCacher.Start();
//        //_embeddingsCacher.IsStarted = true;
    public void Dispose()
    {
        //TODO arek
    }
}
