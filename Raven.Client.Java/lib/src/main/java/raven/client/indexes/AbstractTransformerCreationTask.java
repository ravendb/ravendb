package raven.client.indexes;

import com.mysema.query.types.Expression;

import raven.abstractions.closure.Action2;
import raven.abstractions.indexing.TransformerDefinition;
import raven.client.IDocumentStore;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.ServerClient;
import raven.client.document.DocumentConvention;

/**
 * Base class for creating transformers
 *
 * The naming convention is that underscores in the inherited class names are replaced by slashed
 * For example: Posts_ByName will be saved to Posts/ByName
 */
public abstract class AbstractTransformerCreationTask extends AbstractCommonApiForIndexesAndTransformers {

  private DocumentConvention conventions;
  protected Expression<?> transformResults;

  /**
   * Gets the name of the index.
   * @return
   */
  public String getTransformerName() {
    return getClass().getName().replace('_', '/');
  }

  public DocumentConvention getConventions() {
    return conventions;
  }

  public void setConventions(DocumentConvention convention) {
    this.conventions = convention;
  }

  //TODO: protected RavenJToken Query(string key)

  /**
   * Creates the Transformer definition.
   * @return
   */
  public TransformerDefinition createTransformerDefinition() {
    TransformerDefinition transformerDefinition = new TransformerDefinition();
    transformerDefinition.setName(getTransformerName());
    //TODO:transformerDefinition.setTransformResults(transformResults)
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

    updateIndexInReplication(databaseCommands, conventions, new Action2<ServerClient, String>() {

      @Override
      public void apply(ServerClient commands, String url) {
        commands.directPutTransformer(getTransformerName(), url, transformerDefinition);
      }
    });

  }

  //TODO: public object Include(string key)
  //TODO:   public object Include(IEnumerable<string> key)

}
