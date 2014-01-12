package net.ravendb.client.indexes;

import net.ravendb.abstractions.closure.Action2;
import net.ravendb.abstractions.indexing.TransformerDefinition;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.connection.OperationMetadata;
import net.ravendb.client.connection.ServerClient;
import net.ravendb.client.document.DocumentConvention;

/**
 * Base class for creating transformers
 *
 * The naming convention is that underscores in the inherited class names are replaced by slashed
 * For example: Posts_ByName will be saved to Posts/ByName
 */
public abstract class AbstractTransformerCreationTask extends AbstractCommonApiForIndexesAndTransformers {

  private DocumentConvention conventions;
  protected String transformResults;

  /**
   * Gets the name of the index.
   * @return
   */
  public String getTransformerName() {
    return getClass().getSimpleName().replace('_', '/');
  }

  public DocumentConvention getConventions() {
    return conventions;
  }

  public void setConventions(DocumentConvention convention) {
    this.conventions = convention;
  }

  /**
   * Creates the Transformer definition.
   * @return
   */
  public TransformerDefinition createTransformerDefinition() {
    TransformerDefinition transformerDefinition = new TransformerDefinition();
    transformerDefinition.setName(getTransformerName());
    if (transformResults == null) {
      throw new IllegalStateException("You must define transformerDefinition");
    }
    transformerDefinition.setTransformResults(transformResults);

    return transformerDefinition;
  }

  public void execute(IDocumentStore store) {
    store.executeTransformer(this);
  }

  public void execute(IDatabaseCommands databaseCommands, DocumentConvention documentConvention) {
    this.conventions = documentConvention;
    final TransformerDefinition transformerDefinition = createTransformerDefinition();
    // This code take advantage on the fact that RavenDB will turn an index PUT
    // to a noop of the index already exists and the stored definition matches
    // the new definition.
    databaseCommands.putTransformer(getTransformerName(), transformerDefinition);

    updateIndexInReplication(databaseCommands, conventions, new Action2<ServerClient, OperationMetadata>() {

      @Override
      public void apply(ServerClient commands, OperationMetadata operationMetadata) {
        commands.directPutTransformer(getTransformerName(), operationMetadata, transformerDefinition);
      }
    });

  }

}
