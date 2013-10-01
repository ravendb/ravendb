package raven.abstractions.connection;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import raven.client.utils.encryptors.Encryptor;
import raven.client.utils.encryptors.IAsymmetricalEncryptor;
import raven.client.utils.encryptors.IHashEncryptor;
import raven.client.utils.encryptors.ISymmetricalEncryptor;

public class OAuthHelper {
  public static class Keys {
    public static String EncryptedData = "data";
    public static String APIKeyName = "api key name";
    public static String Challenge = "challenge";
    public static String Response = "response";

    public static String RSAExponent = "exponent";
    public static String RSAModulus = "modulus";

    public static String ChallengeTimestamp = "pepper";
    public static String ChallengeSalt = "salt";
    public static int ChallengeSaltLength = 64;

    public static String ResponseFormat = "%s;%s";
    public static String WWWAuthenticateHeaderKey = "Raven ";
  }

  public static String hash(String data) {
    IHashEncryptor sha1 = Encryptor.getCurrent().createHash();
    return bytesToString(sha1.compute20(data.getBytes()));
  }

  public static String encryptAsymmetric(byte[] exponent, byte[] modulus, String data) {
    try {
      byte[] bytes = data.getBytes();

      ISymmetricalEncryptor aesKeyGen = Encryptor.getCurrent().createSymmetrical(256);
      aesKeyGen.generateKey();
      aesKeyGen.generateIV();

      byte[] encryptedKeyAndIv = addEncryptedKeyAndIv(exponent, modulus, aesKeyGen.getKey(), aesKeyGen.getIV());

      Cipher encryptor = aesKeyGen.createEncryptor();
      byte[] encryptedBytes = encryptor.doFinal(bytes);

      return bytesToString(ArrayUtils.addAll(encryptedKeyAndIv, encryptedBytes));
    } catch (BadPaddingException | IllegalBlockSizeException | NoSuchAlgorithmException | InvalidKeyException | NoSuchPaddingException | InvalidAlgorithmParameterException e) {
      throw new RuntimeException(e);
    }
  }

  private static byte[] addEncryptedKeyAndIv(byte[] exponent, byte[] modulus, byte[] key, byte[] iv) {
    try {
      IAsymmetricalEncryptor rsa = Encryptor.getCurrent().createAsymmetrical(exponent, modulus);
      return rsa.encrypt(ArrayUtils.addAll(key, iv));
    } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException | NoSuchPaddingException | IllegalBlockSizeException | BadPaddingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, String> parseDictionary(String data) {
    String[] tokens = data.split(",");
    Map<String, String> result = new HashMap<>();
    for (String token: tokens) {
      String[] items = StringUtils.splitPreserveAllTokens(token,'=');
      if (items.length > 2) {
        String key = items[0];
        items = Arrays.copyOfRange(items, 1, items.length);
        result.put(key.trim(), StringUtils.join(items, "=").trim());
      } else {
        result.put(items[0].trim(), items[1].trim());
      }
    }
    return result;
  }

  public static String dictionaryToString(Map<String, String> data) {
    List<String> items = new ArrayList<>();
    for (Map.Entry<String, String> entry: data.entrySet()) {
      items.add(entry.getKey() + "=" + entry.getValue());
    }
    return StringUtils.join(items, ",");
  }

  public static byte[] parseBytes(String data) {
    if (data == null) {
      return null;
    }
    return Base64.decodeBase64(data);
  }

  public static String bytesToString(byte[] data) {
    if (data == null) {
      return null;
    }
    return Base64.encodeBase64String(data);
  }


  public static void main(String[] args) throws Exception {
    //TODO: delete me
//    OAuthHelper.encryptAsymmetric(exponent, modulus, data)

    String encodeBase64 = Base64.encodeBase64String(new byte[] { 1,2,3});

    String modulus = "oLG/fa+JA7scV7rj290GM651EzyrAHDG6vyzPwqxAwNgfcTzG4i9RuQ41q+So4Qg9NX1oQHq4ieYx9NjTAeM4qzIQR5QJnj2i4D6FLWMxzUdkliiBC2GDJ/5uxFYK/TPJMvg7+yhayZcXeuiRq8KtK1GBLiAeb1Q3kIEvxn1M6eGgK134Zbxtr8skuViKqxMkJw3OJrqI/IuMnW9aEHEEyA9kjVXokgx1C0u0ypaLxpKCj0mu/KAWc7r5x4M2qjSd0G6TVea4suTt5UM5/Pq/DhmPu2jGHAwxMJ5rfqXRUBtYirgJ/KqIZlZMWuHSbBMUg0aSOmikPycv3uUIbgSbw";
    String exp = "AQAB";

    byte[] decodeModulus = Base64.decodeBase64(modulus);
    byte[] decodeExp = Base64.decodeBase64(exp);

    IAsymmetricalEncryptor encryptor = Encryptor.getCurrent().createAsymmetrical(decodeExp, decodeModulus);
    byte[] encrypt = encryptor.encrypt(new byte[] { 1,2,3});


    String asymmetric = OAuthHelper.encryptAsymmetric(decodeExp, decodeModulus, "XXXX");
    System.out.println(asymmetric);
  }
}
