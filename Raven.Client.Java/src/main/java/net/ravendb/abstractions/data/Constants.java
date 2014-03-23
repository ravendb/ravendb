package net.ravendb.abstractions.data;

import java.util.Locale;
import java.util.UUID;

public class Constants {

  public static final String VERSION = "3.0.0.0";

  public final static String RAVEN_CLIENT_PRIMARY_SERVER_URL = "Raven-Client-Primary-Server-Url";
  public final static String RAVEN_CLIENT_PRIMARY_SERVER_LAST_CHECK = "Raven-Client-Primary-Server-LastCheck";
  public final static String RAVEN_FORCE_PRIMARY_SERVER_CHECK = "Raven-Force-Primary-Server-Check";

  public final static String RAVEN_REPLICATION_DESTINATIONS = "Raven/Replication/Destinations";

  public final static String ALLOW_BUNDLES_CHANGE = "Raven-Temp-Allow-Bundles-Change";

  public final static String LAST_MODIFIED = "Last-Modified";
  public final static String RAVEN_LAST_MODIFIED = "Raven-Last-Modified";
  public final static String SYSTEM_DATABASE = "<system>";
  public final static String TEMPORARY_SCORE_VALUE = "Temp-Index-Score";
  public final static String RANDOM_FIELD_NAME = "__random";
  public final static String NULL_VALUE_NOT_ANALYZED = "[[NULL_VALUE]]";
  public final static String EMPTY_STRING_NOT_ANALYZED = "[[EMPTY_STRING]]";
  public final static String NULL_VALUE = "NULL_VALUE";
  public final static String EMPTY_STRING = "EMPTY_STRING";
  public final static String DOCUMENT_ID_FIELD_NAME = "__document_id";
  public final static String INTERSECT_SEPARATOR = " INTERSECT ";
  public static final String RAVEN_CLR_TYPE = "Raven-Clr-Type";
  public final static String RAVEN_ENTITY_NAME = "Raven-Entity-Name";
  public static final String RAVEN_READ_ONLY = "Raven-Read-Only";
  public final static String ALL_FIELDS = "__all_fields";
  public final static String RAVEN_DOCUMENT_DOES_NOT_EXISTS = "Raven-Document-Does-Not-Exists";
  public final static String METADATA = "@metadata";


  public final static String RAVEN_LAST_MODIFIED_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSX";
  public final static UUID EMPTY_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  public final static String RAVEN_S_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

  //Spatial
  public final static String DEFAULT_SPATIAL_FIELD_NAME = "__spatial";
  public final static String SPATIAL_SHAPE_FIELD_NAME = "__spatialShape";
  public final static double DEFAULT_SPATIAL_DISTANCE_ERROR_PCT = 0.025d;
  public final static String DISTANCE_FIELD_NAME = "__distance";

  public final static String NEXT_PAGE_START = "Next-Page-Start";


  /**
   * The International Union of Geodesy and Geophysics says the Earth's mean radius in KM is:
   *
   * [1] http://en.wikipedia.org/wiki/Earth_radius
   */

  public final static double EARTH_MEAN_RADIUS_KM = 6371.0087714;
  public final static double MILES_TO_KM = 1.60934;

  public final static String RAVEN_CREATE_VERSION = "Raven-Create-Version";

  public final static Locale getDefaultLocale() {
    return Locale.US;
  }










}
