package net.ravendb.abstractions.oauth;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.connection.OAuthHelper;
import net.ravendb.abstractions.connection.WebRequestEventArgs;
import net.ravendb.abstractions.exceptions.HttpOperationException;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.connection.implementation.HttpJsonRequestFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;


public class SecuredAuthenticator extends AbstractAuthenticator {

  private final String apiKey;
  private HttpJsonRequestFactory jsonRequestFactory;

  public SecuredAuthenticator(String apiKey, HttpJsonRequestFactory jsonRequestFactory) {
    super();
    this.apiKey = apiKey;
    this.jsonRequestFactory = jsonRequestFactory;
  }

  @Override
  public void configureRequest(Object sender, WebRequestEventArgs e) {
    if (currentOauthToken != null) {
      super.configureRequest(sender, e);
      return;
    }
    if (apiKey != null) {
      e.getRequest().setHeader("Has-Api-Key", "true");
    }
  }
  private Tuple<HttpPost, String> prepareOAuthRequest(String oauthSource, String serverRSAExponent, String serverRSAModulus, String challenge, String apiKey) {
    HttpPost authRequest = new HttpPost(oauthSource);
    authRequest.setHeader("grant_type", "client_credentials");
    authRequest.setHeader("Accept", "application/json;charset=UTF-8");

    if (StringUtils.isNotEmpty(serverRSAExponent) && StringUtils.isNotEmpty(serverRSAModulus) && StringUtils.isNotEmpty(challenge)) {
      byte[] exponent = OAuthHelper.parseBytes(serverRSAExponent);
      byte[] modulus = OAuthHelper.parseBytes(serverRSAModulus);

      String[] apiKeyParts = apiKey.split("/");
      if (apiKeyParts.length > 2) {
        apiKeyParts[1] = StringUtils.join(Arrays.copyOfRange(apiKeyParts, 1, apiKeyParts.length - 1), "/");
      }
      if (apiKeyParts.length < 2) {
        throw new IllegalStateException("Invalid API key");
      }
      String apiKeyName = apiKeyParts[0].trim();
      String apiSecret = apiKeyParts[1].trim();

      Map<String, String> toEncryptMap = new HashMap<>();
      toEncryptMap.put(OAuthHelper.Keys.APIKeyName, apiKeyName);
      toEncryptMap.put(OAuthHelper.Keys.Challenge, challenge);
      toEncryptMap.put(OAuthHelper.Keys.Response, OAuthHelper.hash(String.format(OAuthHelper.Keys.ResponseFormat, challenge, apiSecret)));
      String dataToEncrypt = OAuthHelper.dictionaryToString(toEncryptMap);

      Map<String, String> dictionary = new HashMap<>();
      dictionary.put(OAuthHelper.Keys.RSAExponent, serverRSAExponent);
      dictionary.put(OAuthHelper.Keys.RSAModulus, serverRSAModulus);
      dictionary.put(OAuthHelper.Keys.EncryptedData, OAuthHelper.encryptAsymmetric(exponent, modulus, dataToEncrypt));

      String data = OAuthHelper.dictionaryToString(dictionary);

      return Tuple.create(authRequest, data);
    }
    return Tuple.create(authRequest, (String)null);
  }



  @Override
  public Action1<HttpRequest> doOAuthRequest(String oauthSource, String apiKey) {
    String serverRSAExponent = null;
    String serverRSAModulus = null;
    String challenge = null;

    // Note that at two tries will be needed in the normal case.
    // The first try will get back a challenge,
    // the second try will try authentication. If something goes wrong server-side though
    // (e.g. the server was just rebooted or the challenge timed out for some reason), we
    // might get a new challenge back, so we try a third time just in case.

    int tries = 0;
    try {
    while(true) {
      tries++;

      Tuple<HttpPost, String> authRequestTuple = prepareOAuthRequest(oauthSource, serverRSAExponent, serverRSAModulus, challenge, apiKey);
      HttpPost authRequest = authRequestTuple.getItem1();
      if (authRequestTuple.getItem2() != null) {
        authRequest.setEntity(new StringEntity(authRequestTuple.getItem2()));
      } else {
        authRequest.setEntity(new StringEntity(""));
      }

      try {
        HttpClient httpClient = jsonRequestFactory.getHttpClient();
        HttpResponse httpResponse = httpClient.execute(authRequest);
        if (httpResponse.getStatusLine().getStatusCode() >= 300) {
          EntityUtils.consumeQuietly(httpResponse.getEntity());
          throw new HttpOperationException("Invalid response from server", null, authRequest, httpResponse);
        }

        HttpEntity httpEntity = httpResponse.getEntity();
        try {
          String token = IOUtils.toString(httpEntity.getContent());
          RavenJObject jToken = RavenJObject.parse(token);
          currentOauthToken = "Bearer " + jToken;
        } finally {
          EntityUtils.consumeQuietly(httpResponse.getEntity());
        }

        return new Action1<HttpRequest>() {
          @Override
          public void apply(HttpRequest request) {
            request.setHeader("Authorization", currentOauthToken);
          }
        };
      } catch (HttpOperationException e) {
        if (tries > 2) {
         // We've already tried three times and failed
          throw e;
        }
        HttpResponse httpResponse = e.getHttpResponse();
        if (httpResponse == null || httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_PRECONDITION_FAILED) {
          throw e;
        }
        Header header = httpResponse.getFirstHeader("WWW-Authenticate");
        if (header == null || !header.getValue().startsWith(OAuthHelper.Keys.WWWAuthenticateHeaderKey)) {
          throw e;
        }

        Map<String, String> challengeDictionary = OAuthHelper.parseDictionary(header.getValue().substring(OAuthHelper.Keys.WWWAuthenticateHeaderKey.length()).trim());
        serverRSAExponent = challengeDictionary.get(OAuthHelper.Keys.RSAExponent);
        serverRSAModulus = challengeDictionary.get(OAuthHelper.Keys.RSAModulus);
        challenge = challengeDictionary.get(OAuthHelper.Keys.Challenge);

        if (StringUtils.isEmpty(serverRSAExponent) || StringUtils.isEmpty(serverRSAModulus) || StringUtils.isEmpty(challenge)) {
          throw new IllegalStateException("Invalid response from server, could not parse raven authentication information: " + header.getValue());
        }

      }

    }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}
