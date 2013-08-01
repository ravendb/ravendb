package raven.client.connection;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import raven.abstractions.closure.Functions;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.RavenDBAwareTests;
import raven.client.connection.implementation.HttpJsonRequestFactory;
import raven.client.document.DocumentConvention;
import raven.client.listeners.IDocumentConflictListener;
import raven.client.utils.UrlUtils;


public abstract class AbstractReplicationTest extends RavenDBAwareTests{

  protected ServerClient serverClient2;


  @Before
  @Override
  public void init() {
    super.init();

    serverClient2 = new ServerClient(DEFAULT_SERVER_URL_2, convention, null,
      new Functions.StaticFunction1<String, ReplicationInformer>(replicationInformer), null, factory,
      UUID.randomUUID(), new IDocumentConflictListener[0]);

  }


  @BeforeClass
  public static void startServerBefore() throws Exception {
    try {
      startServer(DEFAULT_SERVER_PORT_1);
      startServer(DEFAULT_SERVER_PORT_2);
    } finally {

    }
  }

  @AfterClass
  public static void stopServerAfter() throws Exception {
    try {
      stopServer(DEFAULT_SERVER_PORT_1);
      stopServer(DEFAULT_SERVER_PORT_2);
    } finally {

    }
  }

}
